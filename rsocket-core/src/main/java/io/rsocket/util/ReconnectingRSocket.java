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
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

@SuppressWarnings("unchecked")
public class ReconnectingRSocket implements CoreSubscriber<RSocket>, RSocket, Runnable {

  private final long backoffMinInMillis;
  private final long backoffMaxInMillis;
  private final Mono<? extends RSocket> source;
  private final Scheduler scheduler;
  private final Predicate<Throwable> errorPredicate;
  private final MonoProcessor<Void> onDispose;

  RSocket value;

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
  static final Consumer<RSocket>[] TERMINATED = new Consumer[0];

  static final ClosedChannelException ON_CLOSE_EXCEPTION = new ClosedChannelException();

  public static Builder builder() {
    return new Builder();
  }

  ReconnectingRSocket(
      Mono<? extends RSocket> source,
      Predicate<Throwable> errorPredicate,
      Scheduler scheduler,
      long backoffMinInMillis,
      long backoffMaxInMillis) {

    this.source = source;
    this.backoffMinInMillis = backoffMinInMillis;
    this.backoffMaxInMillis = backoffMaxInMillis;
    this.scheduler = scheduler;
    this.errorPredicate = errorPredicate;
    this.onDispose = MonoProcessor.create();

    SUBSCRIBERS.lazySet(this, EMPTY_UNSUBSCRIBED);
  }

  public void subscribe(Consumer<RSocket> actual) {
    if (!add(actual)) {
      actual.accept(value);
    }
  }

  @Override
  public void onSubscribe(Subscription subscription) {
    subscription.request(Long.MAX_VALUE);
  }

  @Override
  public void onComplete() {
    final RSocket value = this.value;

    if (value == null) {
      reconnect();
    } else {
      value.onClose().subscribe(null, null, () -> resubscribeWhen(ON_CLOSE_EXCEPTION));
      Consumer<RSocket>[] array = SUBSCRIBERS.getAndSet(this, TERMINATED);
      for (Consumer<? super RSocket> as : array) {
        as.accept(value);
      }
    }
  }

  @Override
  public void onError(Throwable t) {
    reconnect();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void onNext(@Nullable RSocket value) {
    this.value = value;
  }

  @Override
  public void run() {
    source.subscribe(this);
  }

  private void reconnect() {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    long nextRandomDelay = random.nextLong(backoffMinInMillis, backoffMaxInMillis);
    scheduler.schedule(this, nextRandomDelay, TimeUnit.MILLISECONDS);
  }

  private boolean resubscribeWhen(Throwable throwable) {
    if (onDispose.isDisposed()) {
      return false;
    }

    if (errorPredicate.test(throwable)) {
      final Consumer<RSocket>[] subscribers = this.subscribers;
      final RSocket current = this.value;
      if ((current == null || current.isDisposed())
          && subscribers == TERMINATED
          && SUBSCRIBERS.compareAndSet(this, TERMINATED, EMPTY_SUBSCRIBED)) {
        this.value = null;
        reconnect();
      }
      return true;
    }
    return false;
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return new FlatMapInner<>(
        this, rsocket -> rsocket.fireAndForget(payload), this::resubscribeWhen);
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return new FlatMapInner<>(
        this, rsocket -> rsocket.requestResponse(payload), this::resubscribeWhen);
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return new FlatMapManyInner<>(
        this, rSocket -> rSocket.requestStream(payload), this::resubscribeWhen);
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return new FlatMapManyInner<>(
        this, rSocket -> rSocket.requestChannel(payloads), this::resubscribeWhen);
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    return new FlatMapInner<>(
        this, rsocket -> rsocket.metadataPush(payload), this::resubscribeWhen);
  }

  @Override
  public double availability() {
    RSocket rsocket = this.value;
    return rsocket != null ? rsocket.availability() : 0d;
  }

  @Override
  public void dispose() {
    onDispose.dispose();
    RSocket value = this.value;
    this.value = null;
    if (value != null) {
      value.dispose();
    }
  }

  @Override
  public boolean isDisposed() {
    return onDispose.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return onDispose;
  }

  boolean add(Consumer<RSocket> ps) {
    for (; ; ) {
      Consumer<RSocket>[] a = subscribers;

      if (a == TERMINATED) {
        return false;
      }

      int n = a.length;
      @SuppressWarnings("unchecked")
      Consumer<RSocket>[] b = new Consumer[n + 1];
      System.arraycopy(a, 0, b, 0, n);
      b[n] = ps;

      if (SUBSCRIBERS.compareAndSet(this, a, b)) {
        if (a == EMPTY_UNSUBSCRIBED) {
          source.subscribe(this);
        }
        return true;
      }
    }
  }

  static final class FlatMapInner<T> extends Mono<T>
      implements CoreSubscriber<T>, Consumer<RSocket>, Subscription, Scannable {

    final ReconnectingRSocket parent;
    final Function<RSocket, Mono<? extends T>> mapper;
    final Predicate<Throwable> errorPredicate;

    boolean done;

    volatile int state;

    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<FlatMapInner> STATE =
        AtomicIntegerFieldUpdater.newUpdater(FlatMapInner.class, "state");

    static final int NONE = 0;
    static final int SUBSCRIBED = 1;
    static final int CANCELLED = 2;

    CoreSubscriber<? super T> actual;
    Subscription s;

    FlatMapInner(
        ReconnectingRSocket parent,
        Function<RSocket, Mono<? extends T>> mapper,
        Predicate<Throwable> errorPredicate) {
      this.parent = parent;
      this.mapper = mapper;
      this.errorPredicate = errorPredicate;
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

      Mono<? extends T> source;
      try {
        source = this.mapper.apply(rSocket);
        source.subscribe(this);
      } catch (Throwable e) {
        Exceptions.throwIfFatal(e);
        Operators.error(actual, e);
      }
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
      if (done) {
        Operators.onNextDropped(payload, actual.currentContext());
        return;
      }
      done = true;
      actual.onNext(payload);
    }

    @Override
    public void onError(Throwable t) {
      if (done) {
        Operators.onErrorDropped(t, actual.currentContext());
        return;
      }

      final CoreSubscriber<? super T> actual = this.actual;

      if (errorPredicate.test(t)) {
        this.actual = null;
        STATE.compareAndSet(this, SUBSCRIBED, NONE);
      } else {
        done = true;
      }
      actual.onError(t);
    }

    @Override
    public void onComplete() {
      if (done) {
        return;
      }
      done = true;
      actual.onComplete();
    }

    @Override
    public void request(long n) {
      s.request(n);
    }

    public void cancel() {
      if (STATE.getAndSet(this, CANCELLED) != CANCELLED) {
        s.cancel();
      }
    }
  }

  static final class FlatMapManyInner<T> extends Flux<T>
      implements CoreSubscriber<T>, Consumer<RSocket>, Subscription, Scannable {

    final ReconnectingRSocket parent;
    final Function<RSocket, Flux<? extends T>> mapper;
    final Predicate<Throwable> errorPredicate;

    boolean done;

    volatile int state;

    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<FlatMapManyInner> STATE =
        AtomicIntegerFieldUpdater.newUpdater(FlatMapManyInner.class, "state");

    static final int NONE = 0;
    static final int SUBSCRIBED = 1;
    static final int CANCELLED = 2;

    CoreSubscriber<? super T> actual;
    Subscription s;

    FlatMapManyInner(
        ReconnectingRSocket parent,
        Function<RSocket, Flux<? extends T>> mapper,
        Predicate<Throwable> errorPredicate) {
      this.parent = parent;
      this.mapper = mapper;
      this.errorPredicate = errorPredicate;
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

      Flux<? extends T> source;
      try {
        source = this.mapper.apply(rSocket);
        source.subscribe(this);
      } catch (Throwable e) {
        Exceptions.throwIfFatal(e);
        Operators.error(actual, e);
      }
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
      actual.onNext(payload);
    }

    @Override
    public void onError(Throwable t) {
      if (done) {
        Operators.onErrorDropped(t, actual.currentContext());
        return;
      }

      final CoreSubscriber<? super T> actual = this.actual;

      if (errorPredicate.test(t)) {
        this.actual = null;
        STATE.compareAndSet(this, SUBSCRIBED, NONE);
      } else {
        done = true;
      }
      actual.onError(t);
    }

    @Override
    public void onComplete() {
      if (done) {
        return;
      }
      done = true;
      actual.onComplete();
    }

    @Override
    public void request(long n) {
      s.request(n);
    }

    public void cancel() {
      if (STATE.getAndSet(this, CANCELLED) != CANCELLED) {
        s.cancel();
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
}
