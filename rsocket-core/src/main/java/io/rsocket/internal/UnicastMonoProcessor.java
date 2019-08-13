package io.rsocket.internal;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

public class UnicastMonoProcessor<O> extends Mono<O>
    implements Processor<O, O>, CoreSubscriber<O>, Disposable, Subscription, Scannable {

  /**
   * Create a {@link MonoProcessor} that will eagerly request 1 on {@link
   * #onSubscribe(Subscription)}, cache and emit the eventual result for 1 or N subscribers.
   *
   * @param <T> type of the expected value
   * @return A {@link MonoProcessor}.
   */
  public static <T> UnicastMonoProcessor<T> create() {
    return new UnicastMonoProcessor<>();
  }

  volatile CoreSubscriber<? super O> actual;

  @SuppressWarnings("rawtypes")
  static final AtomicReferenceFieldUpdater<UnicastMonoProcessor, CoreSubscriber> ACTUAL =
      AtomicReferenceFieldUpdater.newUpdater(
          UnicastMonoProcessor.class, CoreSubscriber.class, "actual");

  volatile int once;

  @SuppressWarnings("rawtypes")
  static final AtomicIntegerFieldUpdater<UnicastMonoProcessor> ONCE =
      AtomicIntegerFieldUpdater.newUpdater(UnicastMonoProcessor.class, "once");

  Publisher<? extends O> source;

  Throwable error;
  volatile boolean terminated;
  O value;

  volatile Subscription subscription;
  static final AtomicReferenceFieldUpdater<UnicastMonoProcessor, Subscription> UPSTREAM =
      AtomicReferenceFieldUpdater.newUpdater(
          UnicastMonoProcessor.class, Subscription.class, "subscription");

  @Override
  public final void cancel() {
    if (isTerminated()) {
      return;
    }

    final Subscription s = UPSTREAM.getAndSet(this, Operators.cancelledSubscription());
    if (s == Operators.cancelledSubscription()) {
      return;
    }

    source = null;
    if (s != null) {
      s.cancel();
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void dispose() {
    final Subscription s = UPSTREAM.getAndSet(this, Operators.cancelledSubscription());
    if (s == Operators.cancelledSubscription()) {
      return;
    }

    final CancellationException e = new CancellationException("Disposed");
    error = e;
    value = null;
    source = null;
    terminated = true;
    if (s != null) {
      s.cancel();
    }

    final CoreSubscriber<? super O> a = this.actual;
    ACTUAL.lazySet(this, null);
    if (a != null) {
      a.onError(e);
    }
  }

  /**
   * Return the produced {@link Throwable} error if any or null
   *
   * @return the produced {@link Throwable} error if any or null
   */
  @Nullable
  public final Throwable getError() {
    return isTerminated() ? error : null;
  }

  /**
   * Indicates whether this {@code MonoProcessor} has been interrupted via cancellation.
   *
   * @return {@code true} if this {@code MonoProcessor} is cancelled, {@code false} otherwise.
   */
  public boolean isCancelled() {
    return isDisposed() && !isTerminated();
  }

  /**
   * Indicates whether this {@code MonoProcessor} has been completed with an error.
   *
   * @return {@code true} if this {@code MonoProcessor} was completed with an error, {@code false}
   *     otherwise.
   */
  public final boolean isError() {
    return getError() != null;
  }

  /**
   * Indicates whether this {@code MonoProcessor} has been terminated by the source producer with a
   * success or an error.
   *
   * @return {@code true} if this {@code MonoProcessor} is successful, {@code false} otherwise.
   */
  public final boolean isTerminated() {
    return terminated;
  }

  @Override
  public boolean isDisposed() {
    return subscription == Operators.cancelledSubscription();
  }

  @Override
  public final void onComplete() {
    onNext(null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public final void onError(Throwable cause) {
    Objects.requireNonNull(cause, "onError cannot be null");

    if (UPSTREAM.getAndSet(this, Operators.cancelledSubscription())
        == Operators.cancelledSubscription()) {
      Operators.onErrorDropped(cause, currentContext());
      return;
    }

    error = cause;
    value = null;
    source = null;
    terminated = true;

    final CoreSubscriber<? super O> a = actual;
    ACTUAL.lazySet(this, null);
    if (a != null) {
      a.onError(cause);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public final void onNext(@Nullable O value) {
    final Subscription s;
    if ((s = UPSTREAM.getAndSet(this, Operators.cancelledSubscription()))
        == Operators.cancelledSubscription()) {
      if (value != null) {
        Operators.onNextDropped(value, currentContext());
      }
      return;
    }

    this.value = value;
    final Publisher<? extends O> parent = source;
    source = null;
    terminated = true;

    final CoreSubscriber<? super O> a = actual;
    ACTUAL.lazySet(this, null);
    if (value == null) {
      if (a != null) {
        a.onComplete();
      }
    } else {
      if (s != null && !(parent instanceof Mono)) {
        s.cancel();
      }

      if (a != null) {
        a.onNext(value);
        a.onComplete();
      }
    }
  }

  @Override
  public final void onSubscribe(Subscription subscription) {
    if (Operators.setOnce(UPSTREAM, this, subscription)) {
      subscription.request(Long.MAX_VALUE);
    }
  }

  /**
   * Returns the value that completed this {@link MonoProcessor}. Returns {@code null} if the {@link
   * MonoProcessor} has not been completed. If the {@link MonoProcessor} is completed with an error
   * a RuntimeException that wraps the error is thrown.
   *
   * @return the value that completed the {@link MonoProcessor}, or {@code null} if it has not been
   *     completed
   * @throws RuntimeException if the {@link MonoProcessor} was completed with an error
   */
  @Nullable
  public O peek() {
    if (!isTerminated()) {
      return null;
    }

    if (value != null) {
      return value;
    }

    if (error != null) {
      RuntimeException re = Exceptions.propagate(error);
      re = Exceptions.addSuppressed(re, new Exception("Mono#peek terminated with an error"));
      throw re;
    }

    return null;
  }

  @Override
  public final void request(long n) {
    Operators.validate(n);
  }

  @Override
  public Context currentContext() {
    final CoreSubscriber<? super O> a = this.actual;
    return a != null ? a.currentContext() : Context.empty();
  }

  @Override
  @Nullable
  public Object scanUnsafe(Attr key) {
    // touch guard
    boolean c = isCancelled();

    if (key == Attr.TERMINATED) {
      return isTerminated();
    }
    if (key == Attr.PARENT) {
      return subscription;
    }
    if (key == Attr.ERROR) {
      return error;
    }
    if (key == Attr.PREFETCH) {
      return Integer.MAX_VALUE;
    }
    if (key == Attr.CANCELLED) {
      return c;
    }
    return null;
  }

  /**
   * Return true if any {@link Subscriber} is actively subscribed
   *
   * @return true if any {@link Subscriber} is actively subscribed
   */
  public final boolean hasDownstream() {
    return actual != null;
  }

  @Override
  public void subscribe(CoreSubscriber<? super O> actual) {
    Objects.requireNonNull(actual, "subscribe");
    if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
      actual.onSubscribe(this);
      ACTUAL.lazySet(this, actual);
      if (isTerminated()) {
        Throwable ex = error;
        if (ex != null) {
          actual.onError(ex);
        } else {
          O v = value;
          if (v != null) {
            actual.onNext(v);
          }
          actual.onComplete();
        }
        ACTUAL.lazySet(this, null);
      }
    } else {
      Operators.error(
          actual,
          new IllegalStateException("UnicastMonoProcessor allows only a single Subscriber"));
    }
  }
}
