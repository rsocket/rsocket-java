/*
 * Copyright 2015-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rsocket.loadbalance;

import io.netty.util.ReferenceCountUtil;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.frame.FrameType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;

class RSocketPool extends ResolvingOperator<Void>
    implements CoreSubscriber<List<LoadbalanceTarget>> {

  final DeferredResolutionRSocket deferredResolutionRSocket = new DeferredResolutionRSocket(this);
  final LoadbalanceStrategy loadbalanceStrategy;
  final Supplier<Stats> statsSupplier;

  volatile PooledRSocket[] activeSockets;

  static final AtomicReferenceFieldUpdater<RSocketPool, PooledRSocket[]> ACTIVE_SOCKETS =
      AtomicReferenceFieldUpdater.newUpdater(
          RSocketPool.class, PooledRSocket[].class, "activeSockets");

  static final PooledRSocket[] EMPTY = new PooledRSocket[0];
  static final PooledRSocket[] TERMINATED = new PooledRSocket[0];

  volatile Subscription s;
  static final AtomicReferenceFieldUpdater<RSocketPool, Subscription> S =
      AtomicReferenceFieldUpdater.newUpdater(RSocketPool.class, Subscription.class, "s");

  RSocketPool(Publisher<List<LoadbalanceTarget>> source, LoadbalanceStrategy loadbalanceStrategy) {
    this.loadbalanceStrategy = loadbalanceStrategy;
    if (loadbalanceStrategy instanceof StatsBasedLoadbalanceStrategy) {
      this.statsSupplier = Stats::new;
    } else {
      this.statsSupplier = () -> Stats.NoOpsStats.INSTANCE;
    }

    ACTIVE_SOCKETS.lazySet(this, EMPTY);

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
   * @param loadbalanceTargets set of newly received unresolved {@link RSocket}s
   */
  @Override
  public void onNext(List<LoadbalanceTarget> loadbalanceTargets) {
    if (isDisposed()) {
      return;
    }

    PooledRSocket[] previouslyActiveSockets;
    PooledRSocket[] activeSockets;
    for (; ; ) {
      HashMap<LoadbalanceTarget, Integer> rSocketSuppliersCopy = new HashMap<>();

      int j = 0;
      for (LoadbalanceTarget loadbalanceTarget : loadbalanceTargets) {
        rSocketSuppliersCopy.put(loadbalanceTarget, j++);
      }

      // checking intersection of active RSocket with the newly received set
      previouslyActiveSockets = this.activeSockets;
      PooledRSocket[] nextActiveSockets =
          new PooledRSocket[previouslyActiveSockets.length + rSocketSuppliersCopy.size()];
      int position = 0;
      for (int i = 0; i < previouslyActiveSockets.length; i++) {
        PooledRSocket rSocket = previouslyActiveSockets[i];

        Integer index = rSocketSuppliersCopy.remove(rSocket.supplier());
        if (index == null) {
          // if one of the active rSockets is not included, we remove it and put in the
          // pending removal
          if (!rSocket.markForRemoval()) {
            nextActiveSockets[position++] = rSocket;
          }
        } else {
          if (rSocket.markActive()) {
            // keep old RSocket instance
            nextActiveSockets[position++] = rSocket;
          } else {
            // put newly create RSocket instance
            nextActiveSockets[position++] =
                new ResolvingPooledRSocket(loadbalanceTargets.get(index), this.statsSupplier.get());
          }
        }
      }

      // going though brightly new rsocket
      for (LoadbalanceTarget newLoadbalanceTarget : rSocketSuppliersCopy.keySet()) {
        nextActiveSockets[position++] =
            new ResolvingPooledRSocket(newLoadbalanceTarget, this.statsSupplier.get());
      }

      // shrank to actual length
      if (position == 0) {
        activeSockets = EMPTY;
      } else {
        activeSockets = Arrays.copyOf(nextActiveSockets, position);
      }

      if (ACTIVE_SOCKETS.compareAndSet(this, previouslyActiveSockets, activeSockets)) {
        break;
      }
    }

    if (isPending()) {
      // notifies that upstream is resolved
      if (activeSockets != EMPTY) {
        //noinspection ConstantConditions
        complete(null);
      }
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

  RSocket select() {
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
  RSocket doSelect() {
    PooledRSocket[] sockets = this.activeSockets;
    if (sockets == EMPTY) {
      return null;
    }

    return this.loadbalanceStrategy.select(sockets);
  }

  static class DeferredResolutionRSocket implements RSocket {

    final RSocketPool parent;

    DeferredResolutionRSocket(RSocketPool parent) {
      this.parent = parent;
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
      return new MonoInner<>(this.parent, payload, FrameType.REQUEST_FNF);
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
      return new MonoInner<>(this.parent, payload, FrameType.REQUEST_RESPONSE);
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
      return new FluxInner<>(this.parent, payload, FrameType.REQUEST_STREAM);
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
      return new FluxInner<>(this.parent, payloads, FrameType.REQUEST_STREAM);
    }

    @Override
    public Mono<Void> metadataPush(Payload payload) {
      return new MonoInner<>(this.parent, payload, FrameType.METADATA_PUSH);
    }
  }

  static final class MonoInner<T> extends MonoDeferredResolution<T, Void> {

    MonoInner(RSocketPool parent, Payload payload, FrameType requestType) {
      super(parent, payload, requestType);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void accept(Void aVoid, Throwable t) {
      if (isTerminated()) {
        return;
      }

      if (t != null) {
        ReferenceCountUtil.safeRelease(this.payload);
        onError(t);
        return;
      }

      RSocketPool parent = (RSocketPool) this.parent;
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
  }

  static final class FluxInner<INPUT> extends FluxDeferredResolution<INPUT, Void> {

    FluxInner(RSocketPool parent, INPUT fluxOrPayload, FrameType requestType) {
      super(parent, fluxOrPayload, requestType);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void accept(Void aVoid, Throwable t) {
      if (isTerminated()) {
        return;
      }

      if (t != null) {
        if (this.requestType == FrameType.REQUEST_STREAM) {
          ReferenceCountUtil.safeRelease(this.fluxOrPayload);
        }
        onError(t);
        return;
      }

      RSocketPool parent = (RSocketPool) this.parent;
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
  }
}
