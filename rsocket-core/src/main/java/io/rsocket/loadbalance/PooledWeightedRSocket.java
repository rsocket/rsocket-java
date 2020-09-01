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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

/** Default implementation of {@link WeightedRSocket} stored in {@link RSocketPool} */
final class PooledWeightedRSocket extends ResolvingOperator<RSocket>
    implements CoreSubscriber<RSocket>, WeightedRSocket {

  final RSocketPool parent;
  final Mono<RSocket> rSocketSource;
  final LoadbalanceTarget loadbalanceTarget;
  final Stats stats;

  volatile Subscription s;

  static final AtomicReferenceFieldUpdater<PooledWeightedRSocket, Subscription> S =
      AtomicReferenceFieldUpdater.newUpdater(PooledWeightedRSocket.class, Subscription.class, "s");

  PooledWeightedRSocket(
      RSocketPool parent,
      Mono<RSocket> rSocketSource,
      LoadbalanceTarget loadbalanceTarget,
      Stats stats) {
    this.parent = parent;
    this.rSocketSource = rSocketSource;
    this.loadbalanceTarget = loadbalanceTarget;
    this.stats = stats;
  }

  @Override
  public void onSubscribe(Subscription s) {
    if (Operators.setOnce(S, this, s)) {
      s.request(Long.MAX_VALUE);
    }
  }

  @Override
  public void onComplete() {
    final Subscription s = this.s;
    final RSocket value = this.value;

    if (s == Operators.cancelledSubscription() || !S.compareAndSet(this, s, null)) {
      this.doFinally();
      return;
    }

    if (value == null) {
      this.terminate(new IllegalStateException("Source completed empty"));
    } else {
      this.complete(value);
    }
  }

  @Override
  public void onError(Throwable t) {
    final Subscription s = this.s;

    if (s == Operators.cancelledSubscription()
        || S.getAndSet(this, Operators.cancelledSubscription())
            == Operators.cancelledSubscription()) {
      this.doFinally();
      Operators.onErrorDropped(t, Context.empty());
      return;
    }

    this.doFinally();
    // terminate upstream which means retryBackoff has exhausted
    this.terminate(t);
  }

  @Override
  public void onNext(RSocket value) {
    if (this.s == Operators.cancelledSubscription()) {
      this.doOnValueExpired(value);
      return;
    }

    this.value = value;
    // volatile write and check on racing
    this.doFinally();
  }

  @Override
  protected void doSubscribe() {
    this.rSocketSource.subscribe(this);
  }

  @Override
  protected void doOnValueResolved(RSocket value) {
    stats.setAvailability(1.0);
    value.onClose().subscribe(null, t -> this.invalidate(), this::invalidate);
  }

  @Override
  protected void doOnValueExpired(RSocket value) {
    stats.setAvailability(0.0);
    value.dispose();
    this.dispose();
  }

  @Override
  public void dispose() {
    super.dispose();
  }

  @Override
  protected void doOnDispose() {
    final RSocketPool parent = this.parent;
    for (; ; ) {
      final PooledWeightedRSocket[] sockets = parent.activeSockets;
      final int activeSocketsCount = sockets.length;

      int index = -1;
      for (int i = 0; i < activeSocketsCount; i++) {
        if (sockets[i] == this) {
          index = i;
          break;
        }
      }

      if (index == -1) {
        break;
      }

      final int lastIndex = activeSocketsCount - 1;
      final PooledWeightedRSocket[] newSockets = new PooledWeightedRSocket[lastIndex];
      if (index != 0) {
        System.arraycopy(sockets, 0, newSockets, 0, index);
      }

      if (index != lastIndex) {
        System.arraycopy(sockets, index + 1, newSockets, index, lastIndex - index);
      }

      if (RSocketPool.ACTIVE_SOCKETS.compareAndSet(parent, sockets, newSockets)) {
        break;
      }
    }
    stats.setAvailability(0.0);
    Operators.terminate(S, this);
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return new RequestTrackingMonoInner<>(this, payload, FrameType.REQUEST_FNF);
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return new RequestTrackingMonoInner<>(this, payload, FrameType.REQUEST_RESPONSE);
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return new RequestTrackingFluxInner<>(this, payload, FrameType.REQUEST_STREAM);
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return new RequestTrackingFluxInner<>(this, payloads, FrameType.REQUEST_CHANNEL);
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    return new RequestTrackingMonoInner<>(this, payload, FrameType.METADATA_PUSH);
  }

  /**
   * Indicates number of active requests
   *
   * @return number of requests in progress
   */
  @Override
  public Stats stats() {
    return stats;
  }

  LoadbalanceTarget target() {
    return loadbalanceTarget;
  }

  @Override
  public double availability() {
    return stats.availability();
  }

  static final class RequestTrackingMonoInner<RESULT>
      extends MonoDeferredResolution<RESULT, RSocket> {

    long startTime;

    RequestTrackingMonoInner(PooledWeightedRSocket parent, Payload payload, FrameType requestType) {
      super(parent, payload, requestType);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void accept(RSocket rSocket, Throwable t) {
      if (isTerminated()) {
        return;
      }

      if (t != null) {
        ReferenceCountUtil.safeRelease(this.payload);
        onError(t);
        return;
      }

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

        startTime = ((PooledWeightedRSocket) parent).stats.startRequest();

        source.subscribe((CoreSubscriber) this);
      } else {
        parent.add(this);
      }
    }

    @Override
    public void onComplete() {
      final long state = this.requested;
      if (state != TERMINATED_STATE && REQUESTED.compareAndSet(this, state, TERMINATED_STATE)) {
        final Stats stats = ((PooledWeightedRSocket) parent).stats;
        final long now = stats.stopRequest(startTime);
        stats.record(now - startTime);
        super.onComplete();
      }
    }

    @Override
    public void onError(Throwable t) {
      final long state = this.requested;
      if (state != TERMINATED_STATE && REQUESTED.compareAndSet(this, state, TERMINATED_STATE)) {
        Stats stats = ((PooledWeightedRSocket) parent).stats;
        stats.stopRequest(startTime);
        stats.recordError(0.0);
        super.onError(t);
      }
    }

    @Override
    public void cancel() {
      long state = REQUESTED.getAndSet(this, STATE_TERMINATED);
      if (state == STATE_TERMINATED) {
        return;
      }

      if (state == STATE_SUBSCRIBED) {
        this.s.cancel();
        ((PooledWeightedRSocket) parent).stats.stopRequest(startTime);
      } else {
        this.parent.remove(this);
        ReferenceCountUtil.safeRelease(this.payload);
      }
    }
  }

  static final class RequestTrackingFluxInner<INPUT>
      extends FluxDeferredResolution<INPUT, RSocket> {

    RequestTrackingFluxInner(
        PooledWeightedRSocket parent, INPUT fluxOrPayload, FrameType requestType) {
      super(parent, fluxOrPayload, requestType);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void accept(RSocket rSocket, Throwable t) {
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

        ((PooledWeightedRSocket) parent).stats.startStream();

        source.subscribe(this);
      } else {
        parent.add(this);
      }
    }

    @Override
    public void onComplete() {
      final long state = this.requested;
      if (state != TERMINATED_STATE && REQUESTED.compareAndSet(this, state, TERMINATED_STATE)) {
        ((PooledWeightedRSocket) parent).stats.stopStream();
        super.onComplete();
      }
    }

    @Override
    public void onError(Throwable t) {
      final long state = this.requested;
      if (state != TERMINATED_STATE && REQUESTED.compareAndSet(this, state, TERMINATED_STATE)) {
        ((PooledWeightedRSocket) parent).stats.stopStream();
        super.onError(t);
      }
    }

    @Override
    public void cancel() {
      long state = REQUESTED.getAndSet(this, STATE_TERMINATED);
      if (state == STATE_TERMINATED) {
        return;
      }

      if (state == STATE_SUBSCRIBED) {
        this.s.cancel();
        ((PooledWeightedRSocket) parent).stats.stopStream();
      } else {
        this.parent.remove(this);
        if (requestType == FrameType.REQUEST_STREAM) {
          ReferenceCountUtil.safeRelease(this.fluxOrPayload);
        }
      }
    }
  }
}
