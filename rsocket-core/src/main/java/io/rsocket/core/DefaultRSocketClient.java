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
package io.rsocket.core;

import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCounted;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.frame.FrameType;
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoOperator;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Sinks;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Default implementation of {@link RSocketClient}
 *
 * @since 1.0.1
 */
class DefaultRSocketClient extends ResolvingOperator<RSocket>
    implements CoreSubscriber<RSocket>, CorePublisher<RSocket>, RSocketClient {
  static final Consumer<ReferenceCounted> DISCARD_ELEMENTS_CONSUMER =
      referenceCounted -> {
        if (referenceCounted.refCnt() > 0) {
          try {
            referenceCounted.release();
          } catch (IllegalReferenceCountException e) {
            // ignored
          }
        }
      };

  static final Object ON_DISCARD_KEY;

  static {
    Context discardAwareContext = Operators.enableOnDiscard(null, DISCARD_ELEMENTS_CONSUMER);
    ON_DISCARD_KEY = discardAwareContext.stream().findFirst().get().getKey();
  }

  final Mono<RSocket> source;

  final Sinks.Empty<Void> onDisposeSink;

  volatile Subscription s;

  static final AtomicReferenceFieldUpdater<DefaultRSocketClient, Subscription> S =
      AtomicReferenceFieldUpdater.newUpdater(DefaultRSocketClient.class, Subscription.class, "s");

  DefaultRSocketClient(Mono<RSocket> source) {
    this.source = unwrapReconnectMono(source);
    this.onDisposeSink = Sinks.empty();
  }

  private Mono<RSocket> unwrapReconnectMono(Mono<RSocket> source) {
    return source instanceof ReconnectMono ? ((ReconnectMono<RSocket>) source).getSource() : source;
  }

  @Override
  public Mono<Void> onClose() {
    return this.onDisposeSink.asMono();
  }

  @Override
  public Mono<RSocket> source() {
    return Mono.fromDirect(this);
  }

  @Override
  public Mono<Void> fireAndForget(Mono<Payload> payloadMono) {
    return new RSocketClientMonoOperator<>(this, FrameType.REQUEST_FNF, payloadMono);
  }

  @Override
  public Mono<Payload> requestResponse(Mono<Payload> payloadMono) {
    return new RSocketClientMonoOperator<>(this, FrameType.REQUEST_RESPONSE, payloadMono);
  }

  @Override
  public Flux<Payload> requestStream(Mono<Payload> payloadMono) {
    return new RSocketClientFluxOperator<>(this, FrameType.REQUEST_STREAM, payloadMono);
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return new RSocketClientFluxOperator<>(this, FrameType.REQUEST_CHANNEL, payloads);
  }

  @Override
  public Mono<Void> metadataPush(Mono<Payload> payloadMono) {
    return new RSocketClientMonoOperator<>(this, FrameType.METADATA_PUSH, payloadMono);
  }

  @Override
  @SuppressWarnings("uncheked")
  public void subscribe(CoreSubscriber<? super RSocket> actual) {
    final ResolvingOperator.MonoDeferredResolutionOperator<RSocket> inner =
        new ResolvingOperator.MonoDeferredResolutionOperator<>(this, actual);
    actual.onSubscribe(inner);

    this.observe(inner);
  }

  @Override
  public void subscribe(Subscriber<? super RSocket> s) {
    subscribe(Operators.toCoreSubscriber(s));
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
    this.source.subscribe(this);
  }

  @Override
  protected void doOnValueResolved(RSocket value) {
    value.onClose().subscribe(null, t -> this.invalidate(), this::invalidate);
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
      value.onClose().subscribe(null, onDisposeSink::tryEmitError, onDisposeSink::tryEmitEmpty);
    } else {
      onDisposeSink.tryEmitEmpty();
    }
  }

  static final class FlatMapMain<R> implements CoreSubscriber<Payload>, Context, Scannable {

    final DefaultRSocketClient parent;
    final CoreSubscriber<? super R> actual;

    final FlattingInner<R> second;

    Subscription s;

    boolean done;

    FlatMapMain(
        DefaultRSocketClient parent, CoreSubscriber<? super R> actual, FrameType requestType) {
      this.parent = parent;
      this.actual = actual;
      this.second = new FlattingInner<>(parent, this, actual, requestType);
    }

    @Override
    public Context currentContext() {
      return this;
    }

    @Override
    public Stream<? extends Scannable> inners() {
      return Stream.of(this.second);
    }

    @Override
    @Nullable
    public Object scanUnsafe(Attr key) {
      if (key == Attr.PARENT) return this.s;
      if (key == Attr.CANCELLED) return this.second.isCancelled();
      if (key == Attr.TERMINATED) return this.done;

      return null;
    }

    @Override
    public void onSubscribe(Subscription s) {
      if (Operators.validate(this.s, s)) {
        this.s = s;
        this.actual.onSubscribe(this.second);
      }
    }

    @Override
    public void onNext(Payload payload) {
      if (this.done) {
        if (payload.refCnt() > 0) {
          try {
            payload.release();
          } catch (IllegalReferenceCountException e) {
            // ignored
          }
        }
        return;
      }
      this.done = true;

      final FlattingInner<R> inner = this.second;

      if (inner.isCancelled()) {
        if (payload.refCnt() > 0) {
          try {
            payload.release();
          } catch (IllegalReferenceCountException e) {
            // ignored
          }
        }
        return;
      }

      inner.payload = payload;

      if (inner.isCancelled()) {
        if (FlattingInner.PAYLOAD.compareAndSet(inner, payload, null)) {
          if (payload.refCnt() > 0) {
            try {
              payload.release();
            } catch (IllegalReferenceCountException e) {
              // ignored
            }
          }
        }
        return;
      }

      this.parent.observe(inner);
    }

    @Override
    public void onError(Throwable t) {
      if (this.done) {
        Operators.onErrorDropped(t, this.actual.currentContext());
        return;
      }
      this.done = true;

      this.actual.onError(t);
    }

    @Override
    public void onComplete() {
      if (this.done) {
        return;
      }
      this.done = true;

      this.actual.onComplete();
    }

    void request(long n) {
      this.s.request(n);
    }

    void cancel() {
      this.s.cancel();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K> K get(Object key) {
      if (key == ON_DISCARD_KEY) {
        return (K) DISCARD_ELEMENTS_CONSUMER;
      }
      return this.actual.currentContext().get(key);
    }

    @Override
    public boolean hasKey(Object key) {
      if (key == ON_DISCARD_KEY) {
        return true;
      }
      return this.actual.currentContext().hasKey(key);
    }

    @Override
    public Context put(Object key, Object value) {
      return this.actual
          .currentContext()
          .put(ON_DISCARD_KEY, DISCARD_ELEMENTS_CONSUMER)
          .put(key, value);
    }

    @Override
    public Context delete(Object key) {
      return this.actual
          .currentContext()
          .put(ON_DISCARD_KEY, DISCARD_ELEMENTS_CONSUMER)
          .delete(key);
    }

    @Override
    public int size() {
      return this.actual.currentContext().size() + 1;
    }

    @Override
    public Stream<Map.Entry<Object, Object>> stream() {
      return Stream.concat(
          Stream.of(
              new AbstractMap.SimpleImmutableEntry<>(ON_DISCARD_KEY, DISCARD_ELEMENTS_CONSUMER)),
          this.actual.currentContext().stream());
    }
  }

  static final class FlattingInner<T> extends DeferredResolution<T, RSocket> {

    final FlatMapMain<T> main;
    final FrameType interactionType;

    volatile Payload payload;

    @SuppressWarnings("rawtypes")
    static final AtomicReferenceFieldUpdater<FlattingInner, Payload> PAYLOAD =
        AtomicReferenceFieldUpdater.newUpdater(FlattingInner.class, Payload.class, "payload");

    FlattingInner(
        DefaultRSocketClient parent,
        FlatMapMain<T> main,
        CoreSubscriber<? super T> actual,
        FrameType interactionType) {
      super(parent, actual);

      this.main = main;
      this.interactionType = interactionType;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void accept(RSocket rSocket, Throwable t) {
      if (this.isCancelled()) {
        return;
      }

      Payload payload = PAYLOAD.getAndSet(this, null);

      // means cancelled
      if (payload == null) {
        return;
      }

      if (t != null) {
        if (payload.refCnt() > 0) {
          try {
            payload.release();
          } catch (IllegalReferenceCountException e) {
            // ignored
          }
        }
        onError(t);
        return;
      }

      CorePublisher<?> source;
      switch (this.interactionType) {
        case REQUEST_FNF:
          source = rSocket.fireAndForget(payload);
          break;
        case REQUEST_RESPONSE:
          source = rSocket.requestResponse(payload);
          break;
        case REQUEST_STREAM:
          source = rSocket.requestStream(payload);
          break;
        case METADATA_PUSH:
          source = rSocket.metadataPush(payload);
          break;
        default:
          this.onError(new IllegalStateException("Should never happen"));
          return;
      }

      source.subscribe((CoreSubscriber) this);
    }

    @Override
    public void request(long n) {
      this.main.request(n);
      super.request(n);
    }

    public void cancel() {
      long state = REQUESTED.getAndSet(this, STATE_CANCELLED);
      if (state == STATE_CANCELLED) {
        return;
      }

      this.main.cancel();

      if (state == STATE_SUBSCRIBED) {
        this.s.cancel();
      } else {
        this.parent.remove(this);
        Payload payload = PAYLOAD.getAndSet(this, null);
        if (payload != null) {
          payload.release();
        }
      }
    }
  }

  static final class RequestChannelInner extends DeferredResolution<Payload, RSocket> {

    final FrameType interactionType;
    final Publisher<Payload> upstream;

    RequestChannelInner(
        DefaultRSocketClient parent,
        Publisher<Payload> upstream,
        CoreSubscriber<? super Payload> actual,
        FrameType interactionType) {
      super(parent, actual);

      this.upstream = upstream;
      this.interactionType = interactionType;
    }

    @Override
    public void accept(RSocket rSocket, Throwable t) {
      if (this.isCancelled()) {
        return;
      }

      if (t != null) {
        onError(t);
        return;
      }

      Flux<Payload> source;
      if (this.interactionType == FrameType.REQUEST_CHANNEL) {
        source = rSocket.requestChannel(this.upstream);
      } else {
        this.onError(new IllegalStateException("Should never happen"));
        return;
      }

      source.subscribe(this);
    }
  }

  static class RSocketClientMonoOperator<T> extends MonoOperator<Payload, T> {

    final DefaultRSocketClient parent;
    final FrameType requestType;

    public RSocketClientMonoOperator(
        DefaultRSocketClient parent, FrameType requestType, Mono<Payload> source) {
      super(source);
      this.parent = parent;
      this.requestType = requestType;
    }

    @Override
    public void subscribe(CoreSubscriber<? super T> actual) {
      this.source.subscribe(new FlatMapMain<T>(this.parent, actual, this.requestType));
    }
  }

  static class RSocketClientFluxOperator<ST extends Publisher<Payload>> extends Flux<Payload> {

    final DefaultRSocketClient parent;
    final FrameType requestType;
    final ST source;

    public RSocketClientFluxOperator(
        DefaultRSocketClient parent, FrameType requestType, ST source) {
      this.parent = parent;
      this.requestType = requestType;
      this.source = source;
    }

    @Override
    public void subscribe(CoreSubscriber<? super Payload> actual) {
      if (requestType == FrameType.REQUEST_CHANNEL) {
        RequestChannelInner inner =
            new RequestChannelInner(this.parent, source, actual, requestType);
        actual.onSubscribe(inner);
        this.parent.observe(inner);
      } else {
        this.source.subscribe(new FlatMapMain<>(this.parent, actual, this.requestType));
      }
    }
  }
}
