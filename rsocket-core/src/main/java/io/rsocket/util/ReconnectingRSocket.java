package io.rsocket.util;

import io.netty.util.internal.ThreadLocalRandom;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.exceptions.ConnectionCloseException;
import io.rsocket.exceptions.ConnectionErrorException;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import io.rsocket.frame.FrameType;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

@SuppressWarnings("unchecked")
public final class ReconnectingRSocket extends ReconnectingSubscriber implements RSocket {

  private final Mono<? extends RSocket> source;
  private final Predicate<Throwable> errorPredicate;
  private final MonoProcessor<Void> onDispose;


  volatile Consumer<RSocket>[] subscribers;
  @SuppressWarnings("rawtypes")
  static final AtomicReferenceFieldUpdater<ReconnectingRSocket, Consumer[]> SUBSCRIBERS =
          AtomicReferenceFieldUpdater.newUpdater(
                  ReconnectingRSocket.class, Consumer[].class, "subscribers");

  @SuppressWarnings("rawtypes")
  static final Consumer<RSocket>[] EMPTY_UNSUBSCRIBED = new Consumer[0];

  @SuppressWarnings("rawtypes")
  static final Consumer<RSocket>[] EMPTY_SUBSCRIBED = new Consumer[0];

  @SuppressWarnings("rawtypes")
  static final Consumer<RSocket>[] READY = new Consumer[0];

  static final int ADDED_STATE = 0;
  static final int READY_STATE = 1;
  static final int TERMINATED_STATE = 2;

  @SuppressWarnings("rawtypes")
  static final Consumer<RSocket>[] TERMINATED = new Consumer[0];

  OnCloseSubscriber onCloseSubscriber;

  public static Builder builder() {
    return new Builder();
  }

  ReconnectingRSocket(
      Mono<? extends RSocket> source,
      Predicate<Throwable> errorPredicate,
      Scheduler scheduler,
      long backoffMinInMillis,
      long backoffMaxInMillis) {

    super(backoffMinInMillis, backoffMaxInMillis, scheduler);

    this.source = source;
    this.errorPredicate = errorPredicate;
    this.onDispose = MonoProcessor.create();



    SUBSCRIBERS.lazySet(this, EMPTY_UNSUBSCRIBED);
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return new FlatMapInner<>(this, payload, FrameType.REQUEST_FNF);
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return new FlatMapInner<>(this, payload, FrameType.REQUEST_RESPONSE);
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return new FlatMapManyInner<>(this, payload, FrameType.REQUEST_STREAM);
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return new FlatMapManyInner<>(this, payloads, FrameType.REQUEST_CHANNEL);
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    return new FlatMapInner<>(this, payload, FrameType.METADATA_PUSH);
  }

  @Override
  public double availability() {
    RSocket rsocket = this.value;
    return rsocket != null ? rsocket.availability() : 0.0d;
  }

  @Override
  public void dispose() {
    Consumer<RSocket>[] consumers = SUBSCRIBERS.getAndSet(this, TERMINATED);

    RSocket value = this.value;
    OnCloseSubscriber onCloseSubscriber = this.onCloseSubscriber;

    this.value = null;
    this.onCloseSubscriber = null;
    this.onDispose.dispose();

    if (value != null) {
      onCloseSubscriber.dispose();
      value.dispose();
    }

    if (consumers != TERMINATED && consumers != READY) {
      for (Consumer<RSocket> consumer : consumers) {
        consumer.accept(null);
      }
    }
  }

  @Override
  public boolean isDisposed() {
    return this.onDispose.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return onDispose;
  }

  void subscribe(Consumer<RSocket> actual) {
    final int state = add(actual);

    if (state == READY_STATE) {
      actual.accept(value);
    } else if (state == TERMINATED_STATE) {
      actual.accept(null);
    }
  }

  @Override
  void complete(RSocket rSocket) {
    onCloseSubscriber = new OnCloseSubscriber(this, rSocket);

    // happens-before write so all non-volatile writes are going to be available on this volatile field read
    Consumer<RSocket>[] consumers = SUBSCRIBERS.getAndSet(this, READY);

    if (consumers == TERMINATED) {
      this.dispose();
      return;
    }

    rSocket.onClose().subscribe(onCloseSubscriber);

    for (Consumer<? super RSocket> as : consumers) {
      as.accept(rSocket);
    }
  }

  boolean tryResubscribe(RSocket rSocket, Throwable throwable) {

    if (this.subscribers == TERMINATED) {
      return false;
    }

    if (this.errorPredicate.test(throwable)) {
      final Consumer<RSocket>[] subscribers = this.subscribers;
      final RSocket currentRSocket = this.value;

      if (currentRSocket == rSocket && subscribers == READY && SUBSCRIBERS.compareAndSet(this, READY, EMPTY_SUBSCRIBED)) {
        // dispose listener to avoid double retry in case reconnection happens earlier that last RSocket is going to be disposed
        this.onCloseSubscriber.dispose();
        this.onCloseSubscriber = null;

        // need to be disposed just in case a given error was considered as one to resubscribe
        if (!currentRSocket.isDisposed()) {
          currentRSocket.dispose();
        }
        this.value = null;

        reconnect();
      }

      return true;
    }
    return false;
  }

  int add(Consumer<RSocket> ps) {
    for (;;) {
      Consumer<RSocket>[] a = this.subscribers;

      if (a == TERMINATED) {
        return TERMINATED_STATE;
      }

      if (a == READY) {
        return READY_STATE;
      }

      int n = a.length;
      @SuppressWarnings("unchecked")
      Consumer<RSocket>[] b = new Consumer[n + 1];
      System.arraycopy(a, 0, b, 0, n);
      b[n] = ps;

      if (SUBSCRIBERS.compareAndSet(this, a, b)) {
        if (a == EMPTY_UNSUBSCRIBED) {
          this.source.subscribe(this);
        }
        return ADDED_STATE;
      }
    }
  }

  @Override
  public void run() {
    this.source.subscribe(this);
  }

  static final class FlatMapInner<T> extends Mono<T>
      implements CoreSubscriber<T>, Consumer<RSocket>, Subscription, Scannable {

    final ReconnectingRSocket parent;
    final FrameType interactionType;
    final Payload payload;

    boolean done;

    volatile int state;
    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<FlatMapInner> STATE =
        AtomicIntegerFieldUpdater.newUpdater(FlatMapInner.class, "state");

    static final int NONE = 0;
    static final int SUBSCRIBED = 1;
    static final int CANCELLED = 2;

    RSocket rSocket;
    CoreSubscriber<? super T> actual;
    Subscription s;

    FlatMapInner(
            ReconnectingRSocket parent,
            Payload payload,
            FrameType interactionType) {
      this.parent = parent;
      this.payload = payload;
      this.interactionType = interactionType;
    }

    @Override
    public void subscribe(CoreSubscriber<? super T> actual) {
      if (state == NONE && STATE.compareAndSet(this, NONE, SUBSCRIBED)) {
        this.actual = actual;
        parent.subscribe(this);
      } else {
        Operators.error(actual, new IllegalStateException("Only a single Subscriber allowed"));
      }
    }

    @Override
    public void accept(RSocket rSocket) {
      if (rSocket == null) {
        Operators.error(actual, new CancellationException("Disposed"));
      }

      this.rSocket = rSocket;

      Mono source;
      switch (interactionType) {
        case REQUEST_FNF:
          source = rSocket.fireAndForget(payload);
          break;
        case REQUEST_RESPONSE:
          source = rSocket.requestResponse(payload);
          break;
        case METADATA_PUSH:
          source = rSocket.metadataPush(payload);
          break;
        default:
          Operators.error(this.actual, new IllegalStateException("Should never happen"));
          return;

      }
      source.subscribe((CoreSubscriber) this);
    }

    @Override
    public Context currentContext() {
      return actual.currentContext();
    }

    @Nullable
    @Override
    public Object scanUnsafe(Attr key) {
      if (key == Attr.PARENT) return s;
      if (key == Attr.ACTUAL) return parent;
      if (key == Attr.TERMINATED) return done;
      if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();

      return null;
    }

    @Override
    public void onSubscribe(Subscription s) {
      this.s = s;
      actual.onSubscribe(this);
    }

    @Override
    public void onNext(T payload) {
      if (this.done) {
        Operators.onNextDropped(payload, this.actual.currentContext());
        return;
      }

      this.done = true;
      this.actual.onNext(payload);
      this.actual.onComplete();
    }

    @Override
    public void onError(Throwable t) {
      if (this.done) {
        Operators.onErrorDropped(t, this.actual.currentContext());
        return;
      }

      final CoreSubscriber<? super T> actual = this.actual;

      if (this.parent.tryResubscribe(rSocket, t)) {
        this.actual = null;
        this.state = NONE;
      } else {
        done = true;
      }
      actual.onError(t);
    }

    @Override
    public void onComplete() {
      if (this.done) {
        return;
      }

      this.done = true;
      this.actual.onComplete();
    }

    @Override
    public void request(long n) {
      this.s.request(n);
    }

    public void cancel() {
      if (STATE.getAndSet(this, CANCELLED) != CANCELLED) {
        this.s.cancel();
      }
    }
  }

  static final class FlatMapManyInner<T> extends Flux<Payload>
      implements CoreSubscriber<Payload>, Consumer<RSocket>, Subscription, Scannable {

    final ReconnectingRSocket parent;
    final FrameType interactionType;
    final T fluxOrPayload;

    volatile int state;
    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<FlatMapManyInner> STATE =
        AtomicIntegerFieldUpdater.newUpdater(FlatMapManyInner.class, "state");
    static final int NONE = 0;
    static final int SUBSCRIBED = 1;
    static final int CANCELLED = 2;


    RSocket rSocket;
    boolean done;
    CoreSubscriber<? super Payload> actual;
    Subscription s;

    FlatMapManyInner(
            ReconnectingRSocket parent,
            T fluxOrPayload,
            FrameType interactionType) {
      this.parent = parent;
      this.fluxOrPayload = fluxOrPayload;
      this.interactionType = interactionType;
    }

    @Override
    public void subscribe(CoreSubscriber<? super Payload> actual) {
      if (this.state == NONE && STATE.compareAndSet(this, NONE, SUBSCRIBED)) {
        this.actual = actual;
        this.parent.subscribe(this);
      } else {
        Operators.error(actual, new IllegalStateException("Only a single Subscriber allowed"));
      }
    }

    @Override
    public void accept(RSocket rSocket) {
      if (rSocket == null) {
        Operators.error(this.actual, new CancellationException("Disposed"));
      }

      this.rSocket = rSocket;

      Flux<? extends Payload> source;
      switch (interactionType) {
        case REQUEST_STREAM:
          source = rSocket.requestStream((Payload) fluxOrPayload);
          break;
        case REQUEST_CHANNEL:
          source = rSocket.requestChannel((Flux<Payload>) fluxOrPayload);
          break;
        default:
          Operators.error(this.actual, new IllegalStateException("Should never happen"));
          return;

      }

      source.subscribe(this);
    }

    @Override
    public Context currentContext() {
      return this.actual.currentContext();
    }

    @Nullable
    @Override
    public Object scanUnsafe(Attr key) {
      int state = this.state;

      if (key == Attr.PARENT) return this.s;
      if (key == Attr.ACTUAL) return this.parent;
      if (key == Attr.TERMINATED) return this.done;
      if (key == Attr.CANCELLED) return state == CANCELLED;

      return null;
    }

    @Override
    public void onSubscribe(Subscription s) {
      this.s = s;
      this.actual.onSubscribe(this);
    }

    @Override
    public void onNext(Payload payload) {
      this.actual.onNext(payload);
    }

    @Override
    public void onError(Throwable t) {
      if (this.done) {
        Operators.onErrorDropped(t, this.actual.currentContext());
        return;
      }

      final CoreSubscriber<? super Payload> actual = this.actual;

      if (this.parent.tryResubscribe(rSocket, t)) {
        this.actual = null;
        this.state = NONE;
      } else {
        this.done = true;
      }
      actual.onError(t);
    }

    @Override
    public void onComplete() {
      if (this.done) {
        return;
      }
      this.done = true;
      this.actual.onComplete();
    }

    @Override
    public void request(long n) {
      this.s.request(n);
    }

    public void cancel() {
      if (STATE.getAndSet(this, CANCELLED) != CANCELLED) {
        this.s.cancel();
      }
    }
  }

  public static class Builder {

    private static final Predicate<Throwable> DEFAULT_ERROR_PREDICATE =
        throwable ->
            throwable instanceof ClosedChannelException
                || throwable instanceof ConnectionCloseException
                || throwable instanceof ConnectionErrorException;

    Duration backoffMin;
    Duration backoffMax;
    Supplier<Mono<RSocket>> sourceSupplier;
    Scheduler scheduler = Schedulers.parallel();
    Predicate<Throwable> errorPredicate = DEFAULT_ERROR_PREDICATE;

    public Builder withSourceRSocket(Supplier<Mono<RSocket>> sourceSupplier) {
      this.sourceSupplier = sourceSupplier;
      return this;
    }

    public Builder withRetryPeriod(Duration period) {
      return withRetryPeriod(period, period);
    }

    public Builder withRetryPeriod(Duration periodMin, Duration periodMax) {
      backoffMin = periodMin;
      backoffMax = periodMax;
      return this;
    }

    public Builder withCustomRetryOnErrorPredicate(Predicate<Throwable> predicate) {
      errorPredicate = predicate;
      return this;
    }

    public Builder withDefaultRetryOnErrorPredicate() {
      return withCustomRetryOnErrorPredicate(DEFAULT_ERROR_PREDICATE);
    }

    public Builder withRetryOnScheduler(Scheduler scheduler) {
      this.scheduler = scheduler;
      return this;
    }

    public ReconnectingRSocket build() {
      Objects.requireNonNull(backoffMin, "Specify required retry period");
      Objects.requireNonNull(backoffMax, "Specify required Max retry period");
      Objects.requireNonNull(errorPredicate, "Specify required retryOnError predicate");
      Objects.requireNonNull(sourceSupplier, "Specify required source RSocket supplier");
      Objects.requireNonNull(scheduler, "Specify required Scheduler");

      return new ReconnectingRSocket(
          Mono.defer(sourceSupplier),
          errorPredicate,
          scheduler,
          backoffMin.toMillis(),
          backoffMax.toMillis());
    }
  }

  final static class OnCloseSubscriber extends BaseSubscriber<Void> {
    final ReconnectingRSocket parent;
    final RSocket rSocket;

    OnCloseSubscriber(ReconnectingRSocket parent, RSocket rSocket) {
      this.parent = parent;
      this.rSocket = rSocket;
    }

    @Override
    protected void hookOnComplete() {
      if (!parent.tryResubscribe(rSocket, ON_CLOSE_EXCEPTION)) {
        this.dispose();
      }
    }
  }
}

abstract class ReconnectingSubscriber implements CoreSubscriber<RSocket>, Runnable, Disposable {

  final long backoffMinInMillis;
  final long backoffMaxInMillis;
  final Scheduler scheduler;

  RSocket value;

  static final ClosedChannelException ON_CLOSE_EXCEPTION = new ClosedChannelException();

  ReconnectingSubscriber(long backoffMinInMillis, long backoffMaxInMillis, Scheduler scheduler) {
    this.backoffMinInMillis = backoffMinInMillis;
    this.backoffMaxInMillis = backoffMaxInMillis;
    this.scheduler = scheduler;
  }

  @Override
  public void onSubscribe(Subscription s) {
    s.request(Long.MAX_VALUE);
  }

  @Override
  public void onComplete() {
    final RSocket value = this.value;

    if (value == null) {
      reconnect();
    } else {
      complete(value);
    }
  }

  abstract void complete(RSocket rSocket);

  @Override
  public void onError(Throwable t) {
    this.value = null;
    reconnect();
  }

  @Override
  public void onNext(@Nullable RSocket value) {
    this.value = value;
  }

  void reconnect() {
    final ThreadLocalRandom random = ThreadLocalRandom.current();
    final long min = this.backoffMinInMillis;
    final long max = this.backoffMaxInMillis;
    final long nextRandomDelay = min == max ? min : random.nextLong(min, max);

    this.scheduler.schedule(this, nextRandomDelay, TimeUnit.MILLISECONDS);
  }
}

