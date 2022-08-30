/*
 * Copyright 2015-2021 the original author or authors.
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
import io.rsocket.Closeable;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.frame.FrameType;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Collectors;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Sinks;
import reactor.util.annotation.Nullable;

class RSocketPool extends ResolvingOperator<Object>
    implements CoreSubscriber<List<LoadbalanceTarget>>, Closeable {

  static final AtomicReferenceFieldUpdater<RSocketPool, PooledRSocket[]> ACTIVE_SOCKETS =
      AtomicReferenceFieldUpdater.newUpdater(
          RSocketPool.class, PooledRSocket[].class, "activeSockets");
  static final PooledRSocket[] EMPTY = new PooledRSocket[0];
  static final PooledRSocket[] TERMINATED = new PooledRSocket[0];
  static final AtomicReferenceFieldUpdater<RSocketPool, Subscription> S =
      AtomicReferenceFieldUpdater.newUpdater(RSocketPool.class, Subscription.class, "s");
  final DeferredResolutionRSocket deferredResolutionRSocket = new DeferredResolutionRSocket(this);
  final RSocketConnector connector;
  final LoadbalanceStrategy loadbalanceStrategy;
  final Sinks.Empty<Void> onAllClosedSink = Sinks.unsafe().empty();
  volatile PooledRSocket[] activeSockets;
  volatile Subscription s;

  public RSocketPool(
      RSocketConnector connector,
      Publisher<List<LoadbalanceTarget>> targetPublisher,
      LoadbalanceStrategy loadbalanceStrategy) {
    this.connector = connector;
    this.loadbalanceStrategy = loadbalanceStrategy;

    ACTIVE_SOCKETS.lazySet(this, EMPTY);

    targetPublisher.subscribe(this);
  }

  @Override
  public Mono<Void> onClose() {
    return onAllClosedSink.asMono();
  }

  @Override
  protected void doOnDispose() {
    Operators.terminate(S, this);

    RSocket[] activeSockets = ACTIVE_SOCKETS.getAndSet(this, TERMINATED);
    for (RSocket rSocket : activeSockets) {
      rSocket.dispose();
    }

    if (activeSockets.length > 0) {
      Mono.whenDelayError(
              Arrays.stream(activeSockets).map(RSocket::onClose).collect(Collectors.toList()))
          .subscribe(null, onAllClosedSink::tryEmitError, onAllClosedSink::tryEmitEmpty);
    } else {
      onAllClosedSink.tryEmitEmpty();
    }
  }

  @Override
  public void onSubscribe(Subscription s) {
    if (Operators.setOnce(S, this, s)) {
      s.request(Long.MAX_VALUE);
    }
  }

  @Override
  public void onNext(List<LoadbalanceTarget> targets) {
    if (isDisposed()) {
      return;
    }

    // This operation should happen less frequently than calls to select() (which are per request)
    // and therefore it is acceptable somewhat less efficient.

    PooledRSocket[] previouslyActiveSockets;
    PooledRSocket[] inactiveSockets;
    PooledRSocket[] socketsToUse;
    for (; ; ) {
      HashMap<LoadbalanceTarget, Integer> rSocketSuppliersCopy = new HashMap<>(targets.size());

      int j = 0;
      for (LoadbalanceTarget target : targets) {
        rSocketSuppliersCopy.put(target, j++);
      }

      // Intersect current and new list of targets and find the ones to keep vs dispose
      previouslyActiveSockets = this.activeSockets;
      inactiveSockets = new PooledRSocket[previouslyActiveSockets.length];
      PooledRSocket[] nextActiveSockets =
          new PooledRSocket[previouslyActiveSockets.length + rSocketSuppliersCopy.size()];
      int activeSocketsPosition = 0;
      int inactiveSocketsPosition = 0;
      for (int i = 0; i < previouslyActiveSockets.length; i++) {
        PooledRSocket rSocket = previouslyActiveSockets[i];

        Integer index = rSocketSuppliersCopy.remove(rSocket.target());
        if (index == null) {
          // if one of the active rSockets is not included, we remove it and put in the
          // pending removal
          if (!rSocket.isDisposed()) {
            inactiveSockets[inactiveSocketsPosition++] = rSocket;
            // TODO: provide a meaningful algo for keeping removed rsocket in the list
            //            nextActiveSockets[position++] = rSocket;
          }
        } else {
          if (!rSocket.isDisposed()) {
            // keep old RSocket instance
            nextActiveSockets[activeSocketsPosition++] = rSocket;
          } else {
            // put newly create RSocket instance
            LoadbalanceTarget target = targets.get(index);
            nextActiveSockets[activeSocketsPosition++] =
                new PooledRSocket(this, this.connector.connect(target.getTransport()), target);
          }
        }
      }

      // The remainder are the brand new targets
      for (LoadbalanceTarget target : rSocketSuppliersCopy.keySet()) {
        nextActiveSockets[activeSocketsPosition++] =
            new PooledRSocket(this, this.connector.connect(target.getTransport()), target);
      }

      if (activeSocketsPosition == 0) {
        socketsToUse = EMPTY;
      } else {
        socketsToUse = Arrays.copyOf(nextActiveSockets, activeSocketsPosition);
      }
      if (ACTIVE_SOCKETS.compareAndSet(this, previouslyActiveSockets, socketsToUse)) {
        break;
      }
    }

    for (PooledRSocket inactiveSocket : inactiveSockets) {
      if (inactiveSocket == null) {
        break;
      }

      inactiveSocket.dispose();
    }

    if (isPending()) {
      // notifies that upstream is resolved
      if (socketsToUse != EMPTY) {
        //noinspection ConstantConditions
        complete(this);
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

        // check since it is possible that between doSelect() and invalidate() we might
        // have received new sockets
        selected = doSelect();
        if (selected != null) {
          return selected;
        }
      }
      return this.deferredResolutionRSocket;
    }

    return selected;
  }

  @Nullable
  RSocket doSelect() {
    PooledRSocket[] sockets = this.activeSockets;

    if (sockets == EMPTY || sockets == TERMINATED) {
      return null;
    }

    return this.loadbalanceStrategy.select(WrappingList.wrap(sockets));
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
      return new FluxInner<>(this.parent, payloads, FrameType.REQUEST_CHANNEL);
    }

    @Override
    public Mono<Void> metadataPush(Payload payload) {
      return new MonoInner<>(this.parent, payload, FrameType.METADATA_PUSH);
    }
  }

  static final class MonoInner<T> extends MonoDeferredResolution<T, Object> {

    MonoInner(RSocketPool parent, Payload payload, FrameType requestType) {
      super(parent, payload, requestType);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void accept(Object aVoid, Throwable t) {
      if (isTerminated()) {
        return;
      }

      if (t != null) {
        ReferenceCountUtil.safeRelease(this.payload);
        onError(t);
        return;
      }

      RSocketPool parent = (RSocketPool) this.parent;
      for (; ; ) {
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

          return;
        }

        final int state = parent.add(this);

        if (state == ADDED_STATE) {
          return;
        }

        if (state == TERMINATED_STATE) {
          final Throwable error = parent.t;
          ReferenceCountUtil.safeRelease(this.payload);
          onError(error);
          return;
        }
      }
    }
  }

  static final class FluxInner<INPUT> extends FluxDeferredResolution<INPUT, Object> {

    FluxInner(RSocketPool parent, INPUT fluxOrPayload, FrameType requestType) {
      super(parent, fluxOrPayload, requestType);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void accept(Object aVoid, Throwable t) {
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
      for (; ; ) {
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

          return;
        }

        final int state = parent.add(this);

        if (state == ADDED_STATE) {
          return;
        }

        if (state == TERMINATED_STATE) {
          final Throwable error = parent.t;
          if (this.requestType == FrameType.REQUEST_STREAM) {
            ReferenceCountUtil.safeRelease(this.fluxOrPayload);
          }
          onError(error);
          return;
        }
      }
    }
  }

  static final class WrappingList implements List<RSocket> {

    static final ThreadLocal<WrappingList> INSTANCE = ThreadLocal.withInitial(WrappingList::new);

    private PooledRSocket[] activeSockets;

    static List<RSocket> wrap(PooledRSocket[] activeSockets) {
      final WrappingList sockets = INSTANCE.get();
      sockets.activeSockets = activeSockets;
      return sockets;
    }

    @Override
    public RSocket get(int index) {
      final PooledRSocket socket = activeSockets[index];

      RSocket realValue = socket.value;
      if (realValue != null) {
        return realValue;
      }

      realValue = socket.valueIfResolved();
      if (realValue != null) {
        return realValue;
      }

      return socket;
    }

    @Override
    public int size() {
      return activeSockets.length;
    }

    @Override
    public boolean isEmpty() {
      return activeSockets.length == 0;
    }

    @Override
    public Object[] toArray() {
      return activeSockets;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] a) {
      return (T[]) activeSockets;
    }

    @Override
    public boolean contains(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<RSocket> iterator() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(RSocket weightedRSocket) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends RSocket> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(int index, Collection<? extends RSocket> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }

    @Override
    public RSocket set(int index, RSocket element) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void add(int index, RSocket element) {
      throw new UnsupportedOperationException();
    }

    @Override
    public RSocket remove(int index) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int indexOf(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int lastIndexOf(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ListIterator<RSocket> listIterator() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ListIterator<RSocket> listIterator(int index) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<RSocket> subList(int fromIndex, int toIndex) {
      throw new UnsupportedOperationException();
    }
  }
}
