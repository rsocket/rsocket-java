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
import reactor.core.publisher.Sinks;
import reactor.util.context.Context;

/** Default implementation of {@link RSocket} stored in {@link RSocketPool} */
final class PooledRSocket extends ResolvingOperator<RSocket>
    implements CoreSubscriber<RSocket>, RSocket {

  final RSocketPool parent;
  final Mono<RSocket> rSocketSource;
  final LoadbalanceTarget loadbalanceTarget;
  final Sinks.Empty<Void> onCloseSink;

  volatile Subscription s;

  static final AtomicReferenceFieldUpdater<PooledRSocket, Subscription> S =
      AtomicReferenceFieldUpdater.newUpdater(PooledRSocket.class, Subscription.class, "s");

  PooledRSocket(
      RSocketPool parent, Mono<RSocket> rSocketSource, LoadbalanceTarget loadbalanceTarget) {
    this.parent = parent;
    this.rSocketSource = rSocketSource;
    this.loadbalanceTarget = loadbalanceTarget;
    this.onCloseSink = Sinks.unsafe().empty();
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
    // terminate upstream (retryBackoff has exhausted) and remove from the parent target list
    this.doCleanup(t);
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
    value.onClose().subscribe(null, this::doCleanup, () -> doCleanup(ON_DISPOSE));
  }

  void doCleanup(Throwable t) {
    if (isDisposed()) {
      return;
    }

    this.terminate(t);

    final RSocketPool parent = this.parent;
    for (; ; ) {
      final PooledRSocket[] sockets = parent.activeSockets;
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

      final PooledRSocket[] newSockets;
      if (activeSocketsCount == 1) {
        newSockets = RSocketPool.EMPTY;
      } else {
        final int lastIndex = activeSocketsCount - 1;

        newSockets = new PooledRSocket[lastIndex];
        if (index != 0) {
          System.arraycopy(sockets, 0, newSockets, 0, index);
        }

        if (index != lastIndex) {
          System.arraycopy(sockets, index + 1, newSockets, index, lastIndex - index);
        }
      }

      if (RSocketPool.ACTIVE_SOCKETS.compareAndSet(parent, sockets, newSockets)) {
        break;
      }
    }

    if (t == ON_DISPOSE) {
      this.onCloseSink.tryEmitEmpty();
    } else {
      this.onCloseSink.tryEmitError(t);
    }
  }

  @Override
  protected void doOnValueExpired(RSocket value) {
    value.dispose();
  }

  @Override
  protected void doOnDispose() {
    Operators.terminate(S, this);

    final RSocket value = this.value;
    if (value != null) {
      value.onClose().subscribe(null, onCloseSink::tryEmitError, onCloseSink::tryEmitEmpty);
    } else {
      onCloseSink.tryEmitEmpty();
    }
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return new MonoInner<>(this, payload, FrameType.REQUEST_FNF);
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return new MonoInner<>(this, payload, FrameType.REQUEST_RESPONSE);
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return new FluxInner<>(this, payload, FrameType.REQUEST_STREAM);
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return new FluxInner<>(this, payloads, FrameType.REQUEST_CHANNEL);
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    return new MonoInner<>(this, payload, FrameType.METADATA_PUSH);
  }

  LoadbalanceTarget target() {
    return this.loadbalanceTarget;
  }

  @Override
  public Mono<Void> onClose() {
    return this.onCloseSink.asMono();
  }

  @Override
  public double availability() {
    final RSocket socket = valueIfResolved();
    return socket != null ? socket.availability() : 0.0d;
  }

  static final class MonoInner<RESULT> extends MonoDeferredResolution<RESULT, RSocket> {

    MonoInner(PooledRSocket parent, Payload payload, FrameType requestType) {
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

        source.subscribe((CoreSubscriber) this);
      } else {
        parent.observe(this);
      }
    }
  }

  static final class FluxInner<INPUT> extends FluxDeferredResolution<INPUT, RSocket> {

    FluxInner(PooledRSocket parent, INPUT fluxOrPayload, FrameType requestType) {
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

        source.subscribe(this);
      } else {
        parent.observe(this);
      }
    }
  }
}
