package io.rsocket.addons;

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
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

public class ResolvingRSocket extends ResolvingOperator<RSocket> implements RSocket {

  final Mono<? extends RSocket> source;
  final MonoProcessor<Void> onDispose;
  final ResolvingMainSubscriber subscriber;

  public ResolvingRSocket(Mono<? extends RSocket> source) {
    this.source = source;
    this.subscriber = new ResolvingMainSubscriber(this);
    this.onDispose = MonoProcessor.create();
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    RSocket rSocket = this.valueIfResolved();
    if (rSocket != null) {
      return rSocket.fireAndForget(payload);
    }

    return new FlatMapInner<>(this, payload, FrameType.REQUEST_FNF);
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    RSocket rSocket = this.valueIfResolved();
    if (rSocket != null) {
      return rSocket.requestResponse(payload);
    }

    return new FlatMapInner<>(this, payload, FrameType.REQUEST_RESPONSE);
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    RSocket rSocket = this.valueIfResolved();
    if (rSocket != null) {
      return rSocket.requestStream(payload);
    }

    return new FlatMapManyInner<>(this, payload, FrameType.REQUEST_STREAM);
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    RSocket rSocket = this.valueIfResolved();
    if (rSocket != null) {
      return rSocket.requestChannel(payloads);
    }

    return new FlatMapManyInner<>(this, payloads, FrameType.REQUEST_CHANNEL);
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    RSocket rSocket = this.valueIfResolved();
    if (rSocket != null) {
      return rSocket.metadataPush(payload);
    }

    return new FlatMapInner<>(this, payload, FrameType.METADATA_PUSH);
  }

  @Override
  public double availability() {
    RSocket rSocket = this.valueIfResolved();
    if (rSocket != null) {
      return rSocket.availability();
    } else {
      return 0.0d;
    }
  }

  @Override
  public Mono<Void> onClose() {
    return onDispose;
  }

  @Override
  protected void doOnDispose() {
    this.subscriber.dispose();

    if (t == ON_DISPOSE) {
      onDispose.onComplete();
    } else {
      onDispose.onError(t);
    }
  }

  @Override
  protected void doSubscribe() {
    this.source.subscribe(this.subscriber);
  }

  @Override
  protected void doOnValueResolved(RSocket value) {
    value.onClose().subscribe(null, __ -> this.invalidate(), this::invalidate);
  }

  @Override
  protected void doOnValueExpired(RSocket value) {
    value.dispose();
  }

  static final class ResolvingMainSubscriber implements CoreSubscriber<RSocket> {

    final ResolvingRSocket parent;

    volatile Subscription s;

    static final AtomicReferenceFieldUpdater<ResolvingMainSubscriber, Subscription> S =
        AtomicReferenceFieldUpdater.newUpdater(
            ResolvingMainSubscriber.class, Subscription.class, "s");

    ResolvingMainSubscriber(ResolvingRSocket parent) {
      this.parent = parent;
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
      final ResolvingRSocket p = this.parent;
      final RSocket value = p.value;

      if (s == Operators.cancelledSubscription() || !S.compareAndSet(this, s, null)) {
        p.doFinally();
        return;
      }

      if (value == null) {
        p.terminate(new IllegalStateException("Unexpected Completion of the Upstream"));
      } else {
        p.complete();
      }
    }

    @Override
    public void onError(Throwable t) {
      final Subscription s = this.s;
      final ResolvingRSocket p = this.parent;

      if (s == Operators.cancelledSubscription()
          || S.getAndSet(this, Operators.cancelledSubscription())
              == Operators.cancelledSubscription()) {
        p.doFinally();
        Operators.onErrorDropped(t, Context.empty());
        return;
      }

      // terminate upstream which means retryBackoff has exhausted
      p.terminate(t);
    }

    @Override
    public void onNext(RSocket value) {
      if (this.s == Operators.cancelledSubscription()) {
        value.dispose();
        return;
      }

      final ResolvingRSocket p = this.parent;

      p.value = value;
      // volatile write and check on racing
      p.doFinally();
    }

    void dispose() {
      Operators.terminate(S, this);
    }
  }

  static final class FlatMapInner<T> extends ResolvingOperator.MonoDeferredResolution<T, RSocket> {

    final FrameType interactionType;
    final Payload payload;

    FlatMapInner(ResolvingRSocket parent, Payload payload, FrameType interactionType) {
      super(parent);
      this.payload = payload;
      this.interactionType = interactionType;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void accept(RSocket rSocket, Throwable t) {
      if (this.requested == STATE_CANCELLED) {
        return;
      }

      if (t != null) {
        ReferenceCountUtil.safeRelease(this.payload);
        onError(t);
        return;
      }

      Mono<?> source;
      switch (interactionType) {
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

  static final class FlatMapManyInner<T>
      extends ResolvingOperator.FluxDeferredResolution<Payload, RSocket> {

    final FrameType interactionType;
    final T fluxOrPayload;

    FlatMapManyInner(ResolvingRSocket parent, T fluxOrPayload, FrameType interactionType) {
      super(parent);
      this.fluxOrPayload = fluxOrPayload;
      this.interactionType = interactionType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void accept(RSocket rSocket, Throwable t) {
      if (this.requested == STATE_CANCELLED) {
        return;
      }

      if (t != null) {
        ReferenceCountUtil.safeRelease(this.fluxOrPayload);
        onError(t);
        return;
      }

      Flux<? extends Payload> source;
      switch (this.interactionType) {
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
}
