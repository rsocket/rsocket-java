package io.rsocket.addons;

import io.netty.util.ReferenceCountUtil;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.frame.FrameType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;

abstract class BaseRSocketPool extends ResolvingOperator<Void>
    implements RSocketPool, CoreSubscriber<List<RSocket>> {

  final DeferredResolutionRSocket deferredResolutionRSocket = new DeferredResolutionRSocket(this);

  volatile RSocket[] activeSockets = EMPTY;

  static final AtomicReferenceFieldUpdater<BaseRSocketPool, RSocket[]> ACTIVE_SOCKETS =
      AtomicReferenceFieldUpdater.newUpdater(
          BaseRSocketPool.class, RSocket[].class, "activeSockets");

  static final RSocket[] EMPTY = new RSocket[0];
  static final RSocket[] TERMINATED = new RSocket[0];

  volatile Subscription s;
  static final AtomicReferenceFieldUpdater<BaseRSocketPool, Subscription> S =
      AtomicReferenceFieldUpdater.newUpdater(BaseRSocketPool.class, Subscription.class, "s");

  BaseRSocketPool(Publisher<List<RSocket>> source) {
    source.subscribe(this);
  }

  @Override
  protected void doOnDispose() {
    Operators.terminate(S, this);

    RSocket[] activeSockets = ACTIVE_SOCKETS.getAndSet(this, TERMINATED);
    for (RSocket rSocket : activeSockets) {
      rSocket.dispose();
    }
  }

  @Override
  public void onSubscribe(Subscription s) {
    if (Operators.setOnce(S, this, s)) {
      s.request(Long.MAX_VALUE);
    }
  }

  /**
   * This operation should happen rarely relatively compares the number of the {@link #select()}
   * method invocations, therefore it is acceptable to have it algorithmically inefficient. The
   * algorithmic complexity of this method is
   *
   * @param sockets set of newly received unresolved {@link RSocket}s
   */
  @Override
  public void onNext(List<RSocket> sockets) {
    if (isDisposed()) {
      return;
    }

    for (; ; ) {
      HashMap<RSocket, Integer> socketsCopy = new HashMap<>();

      int j = 0;
      for (RSocket rSocket : sockets) {
        socketsCopy.put(rSocket, j++);
      }

      // checking intersection of active RSocket with the newly received set
      RSocket[] activeSockets = this.activeSockets;
      RSocket[] nextActiveSockets = new RSocket[activeSockets.length + socketsCopy.size()];
      int position = 0;
      for (int i = 0; i < activeSockets.length; i++) {
        RSocket rSocket = activeSockets[i];

        Integer index = socketsCopy.remove(rSocket);
        if (index == null) {
          // if one of the active rSockets is not included, we remove it and put in the
          // pending removal
          // FIXME: create PooledRSocket to check whether it has to be disposed or put to
          //  pending depends on number of ongoing calls, etc.
          //          if (!rSocket.markPendingRemoval()) {
          //            nextActiveSockets[position++] = rSocket;
          //          }
          if (!rSocket.isDisposed()) {
            nextActiveSockets[position++] = rSocket;
          }
        } else {
          if (rSocket.isDisposed()) {
            // put newly create RSocket instance
            nextActiveSockets[position++] = sockets.get(index);
          } else {
            // keep old RSocket instance
            nextActiveSockets[position++] = rSocket;
          }
        }
      }

      // going though brightly new rsocket
      for (RSocket newRSocket : socketsCopy.keySet()) {
        nextActiveSockets[position++] = newRSocket;
      }

      // shrank to actual length
      RSocket[] shrankCopy;
      if (position == 0) {
        shrankCopy = EMPTY;
      } else {
        shrankCopy = Arrays.copyOf(nextActiveSockets, position);
      }

      if (ACTIVE_SOCKETS.compareAndSet(this, activeSockets, shrankCopy)) {
        break;
      }
    }

    if (isPending()) {
      // notifies that upstream is resolved
      complete();
    }
  }

  @Override
  public void onError(Throwable t) {
    // indicates upstream termination
    S.set(this, Operators.cancelledSubscription());
    // propagates error and terminates the whole pool
    terminate(t);
  }

  @Override
  public void onComplete() {
    // indicates upstream termination
    S.set(this, Operators.cancelledSubscription());
  }

  @Override
  public RSocket select() {
    if (isDisposed()) {
      return this.deferredResolutionRSocket;
    }

    RSocket selected = doSelect();

    if (selected == null) {
      if (this.s == Operators.cancelledSubscription()) {
        terminate(new CancellationException("Pool is exhausted"));
      } else {
        invalidate();
      }
      return this.deferredResolutionRSocket;
    }

    return selected;
  }

  @Nullable
  abstract RSocket doSelect();

  static class DeferredResolutionRSocket implements RSocket {

    final BaseRSocketPool parent;

    DeferredResolutionRSocket(BaseRSocketPool parent) {
      this.parent = parent;
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
      return new PooledMonoInner<>(this.parent, payload, FrameType.REQUEST_FNF);
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
      return new PooledMonoInner<>(this.parent, payload, FrameType.REQUEST_RESPONSE);
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
      return new PooledFluxInner<>(this.parent, payload, FrameType.REQUEST_STREAM);
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
      return new PooledFluxInner<>(this.parent, payloads, FrameType.REQUEST_STREAM);
    }

    @Override
    public Mono<Void> metadataPush(Payload payload) {
      return new PooledMonoInner<>(this.parent, payload, FrameType.METADATA_PUSH);
    }
  }

  static class PooledMonoInner<T> extends ResolvingOperator.MonoDeferredResolution<T, Void> {

    final BaseRSocketPool parent;
    final Payload payload;
    final FrameType requestType;

    PooledMonoInner(BaseRSocketPool parent, Payload payload, FrameType requestType) {
      super(parent);
      this.parent = parent;
      this.payload = payload;
      this.requestType = requestType;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void accept(Void aVoid, Throwable t) {
      if (this.requested == STATE_CANCELLED) {
        return;
      }

      if (t != null) {
        ReferenceCountUtil.safeRelease(this.payload);
        onError(t);
        return;
      }

      BaseRSocketPool parent = this.parent;
      RSocket rSocket = parent.doSelect();
      if (rSocket != null) {
        Mono<?> source;
        switch (this.requestType) {
          case REQUEST_FNF:
            source = rSocket.fireAndForget(this.payload);
            break;
          case REQUEST_RESPONSE:
            source = rSocket.requestResponse(this.payload);
            break;
          case METADATA_PUSH:
            source = rSocket.metadataPush(this.payload);
            break;
          default:
            Operators.error(this.actual, new IllegalStateException("Should never happen"));
            return;
        }

        source.subscribe((CoreSubscriber) this);
      } else {
        parent.add(this);
      }
    }

    public void cancel() {
      long state = REQUESTED.getAndSet(this, STATE_CANCELLED);
      if (state == STATE_CANCELLED) {
        return;
      }

      if (state == STATE_SUBSCRIBED) {
        this.s.cancel();
      } else {
        this.parent.remove(this);
        ReferenceCountUtil.safeRelease(this.payload);
      }
    }
  }

  static class PooledFluxInner<T> extends ResolvingOperator.FluxDeferredResolution<Payload, Void> {

    final BaseRSocketPool parent;
    final T fluxOrPayload;
    final FrameType requestType;

    PooledFluxInner(BaseRSocketPool parent, T fluxOrPayload, FrameType requestType) {
      super(parent);
      this.parent = parent;
      this.fluxOrPayload = fluxOrPayload;
      this.requestType = requestType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void accept(Void aVoid, Throwable t) {
      if (this.requested == STATE_CANCELLED) {
        return;
      }

      if (t != null) {
        ReferenceCountUtil.safeRelease(this.fluxOrPayload);
        onError(t);
        return;
      }

      BaseRSocketPool parent = this.parent;
      RSocket rSocket = parent.doSelect();
      if (rSocket != null) {
        Flux<? extends Payload> source;
        switch (this.requestType) {
          case REQUEST_STREAM:
            source = rSocket.requestStream((Payload) this.fluxOrPayload);
            break;
          case REQUEST_CHANNEL:
            source = rSocket.requestChannel((Flux<Payload>) this.fluxOrPayload);
            break;
          default:
            Operators.error(this.actual, new IllegalStateException("Should never happen"));
            return;
        }

        source.subscribe(this);
      } else {
        parent.add(this);
      }
    }

    public void cancel() {
      long state = REQUESTED.getAndSet(this, STATE_CANCELLED);
      if (state == STATE_CANCELLED) {
        return;
      }

      if (state == STATE_SUBSCRIBED) {
        this.s.cancel();
      } else {
        this.parent.remove(this);
        ReferenceCountUtil.safeRelease(this.fluxOrPayload);
      }
    }
  }

  /** Specific interface for all RSocket store in {@link RSocketPool} */
  public static interface PooledRSocket extends RSocket {

    /**
     * Indicates number of active requests
     *
     * @return number of requests in progress
     */
    int activeRequests();

    /**
     * Try to dispose this instance if possible. Otherwise, if there is ongoing requests, mark this
     * as pending for removal and dispose once all the requests are terminated.<br>
     * This operation may be cancelled if {@link #markActive()} is invoked prior this instance has
     * been disposed
     *
     * @return {@code true} if this instance was disposed
     */
    boolean markForRemoval();

    /**
     * Try to restore state of this RSocket to be active after marking as pending removal again.
     *
     * @return {@code true} if marked as active. Otherwise, should be treated as it was disposed.
     */
    boolean markActive();
  }
}
