package io.rsocket.internal;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

public class UnicastMonoProcessor<O> extends Mono<O>
    implements Processor<O, O>, CoreSubscriber<O>, Fuseable, Disposable, Fuseable.QueueSubscription<O>, Scannable {

  /**
   * Create a {@link UnicastMonoProcessor} that will eagerly request 1 on {@link
   * #onSubscribe(Subscription)}, cache and emit the eventual result for 1 or N subscribers.
   *
   * @param <T> type of the expected value
   * @return A {@link UnicastMonoProcessor}.
   */
  public static <T> UnicastMonoProcessor<T> create() {
    return new UnicastMonoProcessor<>();
  }

  /**
   * Indicates this Subscription has no value and not requested yet.
   */
  static final int NO_SUBSCRIBER_NO_RESULT  = 0;
  /**
   * Indicates this Subscription has no value and not requested yet.
   */
  static final int NO_SUBSCRIBER_HAS_RESULT = 1;
  /**
   * Indicates this Subscription has no value and not requested yet.
   */
  static final int NO_REQUEST_NO_RESULT     = 4;
  /**
   * Indicates this Subscription has a value but not requested yet.
   */
  static final int NO_REQUEST_HAS_RESULT    = 5;
  /**
   * Indicates this Subscription has been requested but there is no value yet.
   */
  static final int HAS_REQUEST_NO_RESULT    = 6;
  /**
   * Indicates this Subscription has both request and value.
   */
  static final int HAS_REQUEST_HAS_RESULT   = 7;
  /**
   * Indicates the Subscription has been cancelled.
   */
  static final int CANCELLED                = 8;
  /**
   * Indicates this Subscription is in fusion mode and is currently empty.
   */
  static final int FUSED_EMPTY              = 16;
  /**
   * Indicates this Subscription is in fusion mode and has an undelivered notification about present result.
   */
  static final int FUSED_HAS_RESULT         = 32;
  /**
   * Indicates this Subscription is in fusion mode and has a result.
   */
  static final int FUSED_READY              = 64;
  /**
   * Indicates this Subscription is in fusion mode and its value has been consumed.
   */
  static final int FUSED_CONSUMED           = 128;

  volatile int state;
  @SuppressWarnings("rawtypes")
  static final AtomicIntegerFieldUpdater<UnicastMonoProcessor> STATE =
      AtomicIntegerFieldUpdater.newUpdater(UnicastMonoProcessor.class, "state");

  volatile int once;
  @SuppressWarnings("rawtypes")
  static final AtomicIntegerFieldUpdater<UnicastMonoProcessor> ONCE =
          AtomicIntegerFieldUpdater.newUpdater(UnicastMonoProcessor.class, "once");

  volatile Subscription subscription;
  @SuppressWarnings("rawtypes")
  static final AtomicReferenceFieldUpdater<UnicastMonoProcessor, Subscription> UPSTREAM =
          AtomicReferenceFieldUpdater.newUpdater(
                  UnicastMonoProcessor.class, Subscription.class, "subscription");

  CoreSubscriber<? super O> actual;

  Throwable error;
  O value;

  UnicastMonoProcessor() {
  }

  @Override
  @NonNull
  public Context currentContext() {
    final CoreSubscriber<? super O> a = this.actual;
    return a != null ? a.currentContext() : Context.empty();
  }

  @Override
  public final void onSubscribe(Subscription subscription) {
    if (Operators.setOnce(UPSTREAM, this, subscription)) {
      subscription.request(Long.MAX_VALUE);
    }
  }

  @Override
  public final void onComplete() {
    onNext(null);
  }

  @Override
  public final void onError(Throwable cause) {
    Objects.requireNonNull(cause, "onError cannot be null");

    if (UPSTREAM.getAndSet(this, Operators.cancelledSubscription())
        == Operators.cancelledSubscription()) {
      Operators.onErrorDropped(cause, currentContext());
      return;
    }

    complete(cause);
  }

  @Override
  public final void onNext(@Nullable O value) {
    final Subscription s;
    if ((s = UPSTREAM.getAndSet(this, Operators.cancelledSubscription()))
        == Operators.cancelledSubscription()) {
      if (value != null) {
        Operators.onNextDropped(value, currentContext());
      }
      return;
    }

    if (value == null) {
      complete();
    } else {
      if (s != null) {
        s.cancel();
      }

      complete(value);
    }
  }

  /**
   * Tries to emit the value and complete the underlying subscriber or
   * stores the value away until there is a request for it.
   * <p>
   * Make sure this method is called at most once
   * @param v the value to emit
   */
  private void complete(O v) {
    for (;;) {
      int state = this.state;

      if (state == FUSED_EMPTY) {
        setValue(v);
        //sync memory since setValue is non volatile
        if (STATE.compareAndSet(this, FUSED_EMPTY, FUSED_READY)) {
          Subscriber<? super O> a = actual;
          a.onNext(v);
          a.onComplete();
          return;
        }
        //refresh state if race occurred so we test if cancelled in the next comparison
        state = this.state;
      }

      // if state is >= HAS_CANCELLED or bit zero is set (*_HAS_VALUE) case, return
      if ((state & ~HAS_REQUEST_NO_RESULT) != 0) {
        this.value = null;
        Operators.onDiscard(v, currentContext());
        return;
      }

      if (state == HAS_REQUEST_NO_RESULT && STATE.compareAndSet(this, HAS_REQUEST_NO_RESULT, HAS_REQUEST_HAS_RESULT)) {
        this.value = null;
        Subscriber<? super O> a = actual;
        a.onNext(v);
        a.onComplete();
        return;
      }
      setValue(v);
      if (state == NO_REQUEST_NO_RESULT && STATE.compareAndSet(this, NO_REQUEST_NO_RESULT, NO_REQUEST_HAS_RESULT)) {
        return;
      }
      if (state == NO_SUBSCRIBER_NO_RESULT && STATE.compareAndSet(this, NO_SUBSCRIBER_NO_RESULT, NO_SUBSCRIBER_HAS_RESULT)) {
        return;
      }
    }
  }

  /**
   * Tries to emit completion the underlying subscriber
   * <p>
   * Make sure this method is called at most once
   */
  private void complete() {
    for (;;) {
      int state = this.state;

      if (state == FUSED_EMPTY) {
        if (STATE.compareAndSet(this, FUSED_EMPTY, FUSED_CONSUMED)) {
          Subscriber<? super O> a = actual;
          a.onComplete();
          return;
        }
        //refresh state if race occurred so we test if cancelled in the next comparison
        state = this.state;
      }

      // if state is >= HAS_CANCELLED or bit zero is set (*_HAS_VALUE) case, return
      if ((state & ~HAS_REQUEST_NO_RESULT) != 0) {
        return;
      }

      if ((state == HAS_REQUEST_NO_RESULT || state == NO_REQUEST_NO_RESULT) && STATE.compareAndSet(this, state, HAS_REQUEST_HAS_RESULT)) {
        Subscriber<? super O> a = actual;
        a.onComplete();
        return;
      }
      if (state == NO_SUBSCRIBER_NO_RESULT && STATE.compareAndSet(this, NO_SUBSCRIBER_NO_RESULT, NO_SUBSCRIBER_HAS_RESULT)) {
        return;
      }
    }
  }

  /**
   * Tries to emit error the underlying subscriber or
   * stores the value away until there is a request for it.
   * <p>
   * Make sure this method is called at most once
   * @param e the error to emit
   */
  private void complete(Throwable e) {
    for (;;) {
      int state = this.state;

      if (state == FUSED_EMPTY) {
        setError(e);
        //sync memory since setValue is non volatile
        if (STATE.compareAndSet(this, FUSED_EMPTY, FUSED_CONSUMED)) {
          Subscriber<? super O> a = actual;
          a.onError(e);
          return;
        }
        //refresh state if race occurred so we test if cancelled in the next comparison
        state = this.state;
      }

      // if state is >= HAS_CANCELLED or bit zero is set (*_HAS_VALUE) case, return
      if ((state & ~HAS_REQUEST_NO_RESULT) != 0) {
        return;
      }

      setError(e);
      if ((state == HAS_REQUEST_NO_RESULT || state == NO_REQUEST_NO_RESULT) && STATE.compareAndSet(this, state, HAS_REQUEST_HAS_RESULT)) {
        Subscriber<? super O> a = actual;
        a.onError(e);
        return;
      }
      if (state == NO_SUBSCRIBER_NO_RESULT && STATE.compareAndSet(this, NO_SUBSCRIBER_NO_RESULT, NO_SUBSCRIBER_HAS_RESULT)) {
        return;
      }
    }
  }

  @Override
  public void subscribe(CoreSubscriber<? super O> actual) {
    Objects.requireNonNull(actual, "subscribe");

    if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
      this.actual = actual;

      // CAS LOOP since there is a racing with [onNext / onComplete / onError / dispose] which can appear at any moment

      int state = this.state;

      // possible states within the racing between [onNext / onComplete / onError / dispose] and setting subscriber
      // are NO_SUBSCRIBER_[NO_RESULT or HAS_RESULT]
      if (state == NO_SUBSCRIBER_NO_RESULT) {
        if (STATE.compareAndSet(this, NO_SUBSCRIBER_NO_RESULT, NO_REQUEST_NO_RESULT)) {
          state = NO_REQUEST_NO_RESULT;
        } else {
          // the possible false position is racing with [onNext / onError / onComplete / dispose]
          // which are going to put the state in the NO_REQUEST_HAS_RESULT
          STATE.set(this, NO_REQUEST_HAS_RESULT);
          state = NO_REQUEST_HAS_RESULT;
        }
      } else {
        STATE.set(this, NO_REQUEST_HAS_RESULT);
        state = NO_REQUEST_HAS_RESULT;
      }


      // check if state is with a result then there is a chance of immediate termination if there is no value
      // e.g. [onError / onComplete / dispose] only
      if (state == NO_REQUEST_HAS_RESULT && this.value == null) {
        this.actual = null;
        Throwable e = this.error;
        // barrier to flush changes
        STATE.set(this, HAS_REQUEST_HAS_RESULT);
        if (e == null) {
          Operators.complete(actual);
        } else {
          Operators.error(actual, e);
        }
        return;
      }

      // call onSubscribe if has value in the result or no result delivered so far
      actual.onSubscribe(this);

      // update state to ensure we observe what happened after onSubscribe appeared
      state = this.state;
      // if FUSED_HAS_RESULT appeared then we have to notify Subscriber that there is a result which MUST be consumed
      if (state == FUSED_HAS_RESULT) {
        this.actual = null;
        Throwable e = this.error;
        if (e != null) {
          // racing between cancel / dispose / complete
          if (STATE.compareAndSet(this, FUSED_HAS_RESULT, FUSED_CONSUMED)) {
            actual.onError(e);
          }
        } else {
          O v = this.value;

          if (v == null) {
            if (STATE.compareAndSet(this, FUSED_HAS_RESULT, FUSED_CONSUMED)) {
              actual.onComplete();
            }
          } else {
            if (STATE.compareAndSet(this, FUSED_HAS_RESULT, FUSED_READY)) {
              actual.onNext(v);
              actual.onComplete();
            }
          }
        }
      }
    } else {
      Operators.error(
              actual,
              new IllegalStateException("UnicastMonoProcessor allows only a single Subscriber"));
    }
  }

  @Override
  public int requestFusion(int mode) {
    if ((mode & ASYNC) != 0) {
      int state = this.state;

      // CAS LOOP since there is a racing with [onNext / onComplete / onError / dispose] which can appear at any moment
      for (;;) {
        if (state == NO_REQUEST_NO_RESULT && STATE.compareAndSet(this, NO_REQUEST_NO_RESULT, FUSED_EMPTY)) {
          break;
        }
        if (state == NO_REQUEST_HAS_RESULT && STATE.compareAndSet(this, NO_REQUEST_HAS_RESULT, FUSED_HAS_RESULT)) {
          break;
        }

        state = this.state;
      }
      return ASYNC;
    }
    return NONE;
  }

  @Override
  public final void request(long n) {
    if (Operators.validate(n)) {
      for (;;) {
        int s = state;
        // if the any bits 1-31 are set, we are either in fusion mode (FUSED_*)
        // or request has been called (HAS_REQUEST_*)
        if ((s & ~NO_REQUEST_HAS_RESULT) != 0) {
          return;
        }
        if (s == NO_REQUEST_HAS_RESULT && STATE.compareAndSet(this, NO_REQUEST_HAS_RESULT, HAS_REQUEST_HAS_RESULT)) {
          O v = value;
          if (v != null) {
            value = null;
            Subscriber<? super O> a = actual;
            a.onNext(v);
            a.onComplete();
          }
          return;
        }
        if (STATE.compareAndSet(this, NO_REQUEST_NO_RESULT, HAS_REQUEST_NO_RESULT)) {
          return;
        }
      }
    }
  }

  @Override
  public final void cancel() {
    if (STATE.getAndSet(this, CANCELLED) <= HAS_REQUEST_NO_RESULT) {
      Operators.onDiscard(value, currentContext());
    }

    final Subscription s = UPSTREAM.getAndSet(this, Operators.cancelledSubscription());
    if (s != null) {
      s.cancel();
    }

    value = null;
    actual = null;
  }

  @Override
  public void dispose() {
    final Subscription s = UPSTREAM.getAndSet(this, Operators.cancelledSubscription());
    if (s == Operators.cancelledSubscription()) {
      return;
    }

    if (s != null) {
      s.cancel();
    }

    complete(new CancellationException("Disposed"));
  }

  @Override
  public O poll() {
    if (STATE.compareAndSet(this, FUSED_READY, FUSED_CONSUMED)) {
      O v = value;
      value = null;
      actual = null;
      return v;
    }
    return null;
  }

  /**
   * Returns the value that completed this {@link UnicastMonoProcessor}. Returns {@code null} if the
   * {@link UnicastMonoProcessor} has not been completed. If the {@link UnicastMonoProcessor} is
   * completed with an error a RuntimeException that wraps the error is thrown.
   *
   * @return the value that completed the {@link UnicastMonoProcessor}, or {@code null} if it has
   *     not been completed
   * @throws RuntimeException if the {@link UnicastMonoProcessor} was completed with an error
   */
  @Nullable
  public O peek() {
    if (!isDisposed()) {
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
  public int size() {
    int state = this.state;

    if (state < FUSED_EMPTY && state % 2 == 1 && this.value != null) {
      return 1;
    } else if (this.value != null) {
      return 1;
    }

    return 0;
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public void clear() {
    this.value = null;
  }

  /**
   * Set the value internally, without impacting request tracking state.
   *
   * @param value the new value.
   * @see #complete(Object)
   */
  private void setValue(O value) {
    this.value = value;
  }

  /**
   * Set the error internally, without impacting request tracking state.
   *
   * @param throwable the error.
   * @see #complete(Object)
   */
  private void setError(Throwable throwable) {
    this.error = throwable;
  }

  /**
   * Return the produced {@link Throwable} error if any or null
   *
   * @return the produced {@link Throwable} error if any or null
   */
  @Nullable
  public final Throwable getError() {
    return isDisposed() ? error : null;
  }

  /**
   * Indicates whether this {@code UnicastMonoProcessor} has been interrupted via cancellation.
   *
   * @return {@code true} if this {@code UnicastMonoProcessor} is cancelled, {@code false}
   *     otherwise.
   */
  public boolean isCancelled() {
    return state == CANCELLED;
  }

  /**
   * Indicates whether this {@code UnicastMonoProcessor} has been completed with an error.
   *
   * @return {@code true} if this {@code UnicastMonoProcessor} was completed with an error, {@code
   *     false} otherwise.
   */
  public final boolean isError() {
    return getError() != null;
  }

  @Override
  public boolean isDisposed() {
    int state = this.state;
    return state == HAS_REQUEST_HAS_RESULT || state == CANCELLED || state == FUSED_CONSUMED;
  }

  @Override
  @Nullable
  public Object scanUnsafe(Attr key) {
    // touch guard
    int state = this.state;

    if (key == Attr.TERMINATED) {
      return state == HAS_REQUEST_HAS_RESULT || state == FUSED_CONSUMED;
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
      return state == CANCELLED;
    }
    return null;
  }

  /**
   * Return true if any {@link Subscriber} is actively subscribed
   *
   * @return true if any {@link Subscriber} is actively subscribed
   */
  public final boolean hasDownstream() {
    return state > NO_SUBSCRIBER_HAS_RESULT && actual != null;
  }
}
