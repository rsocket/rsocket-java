/*
 * Copyright 2015-2019 the original author or authors.
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

package io.rsocket.internal;

import io.rsocket.util.MonoLifecycleHandler;
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
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.SignalType;
import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

public class UnicastMonoProcessor<O> extends Mono<O>
    implements Processor<O, O>, CoreSubscriber<O>, Disposable, Subscription, Scannable {

  static final MonoLifecycleHandler DEFAULT_LIFECYCLE = new MonoLifecycleHandler() {};

  /**
   * Create a {@link UnicastMonoProcessor} that will eagerly request 1 on {@link
   * #onSubscribe(Subscription)}, cache and emit the eventual result for a single subscriber.
   *
   * @param <T> type of the expected value
   * @return A {@link UnicastMonoProcessor}.
   */
  @SuppressWarnings("unchecked")
  public static <T> UnicastMonoProcessor<T> create() {
    return new UnicastMonoProcessor<T>(DEFAULT_LIFECYCLE);
  }

  /**
   * Create a {@link UnicastMonoProcessor} that will eagerly request 1 on {@link
   * #onSubscribe(Subscription)}, cache and emit the eventual result for a single subscriber.
   *
   * @param lifecycleHandler lifecycle handler
   * @param <T> type of the expected value
   * @return A {@link UnicastMonoProcessor}.
   */
  public static <T> UnicastMonoProcessor<T> create(MonoLifecycleHandler<T> lifecycleHandler) {
    return new UnicastMonoProcessor<>(lifecycleHandler);
  }

  /** Indicates this Subscription has no value and not requested yet. */
  static final int NO_SUBSCRIBER_NO_RESULT = 0;
  /** Indicates this Subscription has no value and not requested yet. */
  static final int NO_SUBSCRIBER_HAS_RESULT = 1;
  /** Indicates this Subscription has no value and not requested yet. */
  static final int NO_REQUEST_NO_RESULT = 4;
  /** Indicates this Subscription has a value but not requested yet. */
  static final int NO_REQUEST_HAS_RESULT = 5;
  /** Indicates this Subscription has been requested but there is no value yet. */
  static final int HAS_REQUEST_NO_RESULT = 6;
  /** Indicates this Subscription has both request and value. */
  static final int HAS_REQUEST_HAS_RESULT = 7;
  /** Indicates the Subscription has been cancelled. */
  static final int CANCELLED = 8;

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

  final MonoLifecycleHandler<O> lifecycleHandler;

  UnicastMonoProcessor(MonoLifecycleHandler<O> lifecycleHandler) {
    this.lifecycleHandler = lifecycleHandler;
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
   * Tries to emit the value and complete the underlying subscriber or stores the value away until
   * there is a request for it.
   *
   * <p>Make sure this method is called at most once
   *
   * @param v the value to emit
   */
  private void complete(O v) {
    for (; ; ) {
      int state = this.state;

      // if state is >= HAS_CANCELLED or bit zero is set (*_HAS_VALUE) case, return
      if ((state & ~HAS_REQUEST_NO_RESULT) != 0) {
        this.value = null;
        Operators.onDiscard(v, currentContext());
        return;
      }

      if (state == HAS_REQUEST_NO_RESULT) {
        if (STATE.compareAndSet(this, HAS_REQUEST_NO_RESULT, HAS_REQUEST_HAS_RESULT)) {
          final Subscriber<? super O> a = actual;
          actual = null;
          value = null;
          lifecycleHandler.doOnTerminal(SignalType.ON_COMPLETE, v, null);
          a.onNext(v);
          a.onComplete();
          return;
        }
      }
      setValue(v);
      if (state == NO_REQUEST_NO_RESULT
          && STATE.compareAndSet(this, NO_REQUEST_NO_RESULT, NO_REQUEST_HAS_RESULT)) {
        return;
      }
      if (state == NO_SUBSCRIBER_NO_RESULT
          && STATE.compareAndSet(this, NO_SUBSCRIBER_NO_RESULT, NO_SUBSCRIBER_HAS_RESULT)) {
        return;
      }
    }
  }

  /**
   * Tries to emit completion the underlying subscriber
   *
   * <p>Make sure this method is called at most once
   */
  private void complete() {
    for (; ; ) {
      int state = this.state;

      // if state is >= HAS_CANCELLED or bit zero is set (*_HAS_VALUE) case, return
      if ((state & ~HAS_REQUEST_NO_RESULT) != 0) {
        return;
      }

      if (state == HAS_REQUEST_NO_RESULT || state == NO_REQUEST_NO_RESULT) {
        if (STATE.compareAndSet(this, state, HAS_REQUEST_HAS_RESULT)) {
          final Subscriber<? super O> a = actual;
          actual = null;
          lifecycleHandler.doOnTerminal(SignalType.ON_COMPLETE, null, null);
          a.onComplete();
          return;
        }
      }
      if (state == NO_SUBSCRIBER_NO_RESULT
          && STATE.compareAndSet(this, NO_SUBSCRIBER_NO_RESULT, NO_SUBSCRIBER_HAS_RESULT)) {
        return;
      }
    }
  }

  /**
   * Tries to emit error the underlying subscriber or stores the value away until there is a request
   * for it.
   *
   * <p>Make sure this method is called at most once
   *
   * @param e the error to emit
   */
  private void complete(Throwable e) {
    for (; ; ) {
      int state = this.state;

      // if state is >= HAS_CANCELLED or bit zero is set (*_HAS_VALUE) case, return
      if ((state & ~HAS_REQUEST_NO_RESULT) != 0) {
        return;
      }

      setError(e);
      if (state == HAS_REQUEST_NO_RESULT || state == NO_REQUEST_NO_RESULT) {
        if (STATE.compareAndSet(this, state, HAS_REQUEST_HAS_RESULT)) {
          final Subscriber<? super O> a = actual;
          actual = null;
          lifecycleHandler.doOnTerminal(SignalType.ON_ERROR, null, e);
          a.onError(e);
          return;
        }
      }
      if (state == NO_SUBSCRIBER_NO_RESULT
          && STATE.compareAndSet(this, NO_SUBSCRIBER_NO_RESULT, NO_SUBSCRIBER_HAS_RESULT)) {
        return;
      }
    }
  }

  @Override
  public void subscribe(CoreSubscriber<? super O> actual) {
    Objects.requireNonNull(actual, "subscribe");

    if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
      final MonoLifecycleHandler<O> lh = this.lifecycleHandler;

      lh.doOnSubscribe();

      this.actual = actual;

      int state = this.state;

      // possible states within the racing between [onNext / onComplete / onError / dispose] and
      // setting subscriber
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

      // check if state is with a result then there is a chance of immediate termination if there is
      // no value
      // e.g. [onError / onComplete / dispose] only
      if (state == NO_REQUEST_HAS_RESULT && this.value == null) {
        this.actual = null;
        Throwable e = this.error;
        // barrier to flush changes
        STATE.set(this, HAS_REQUEST_HAS_RESULT);
        if (e == null) {
          lh.doOnTerminal(SignalType.ON_COMPLETE, null, null);
          Operators.complete(actual);
        } else {
          lh.doOnTerminal(SignalType.ON_ERROR, null, e);
          Operators.error(actual, e);
        }
        return;
      }

      // call onSubscribe if has value in the result or no result delivered so far
      actual.onSubscribe(this);
    } else {
      Operators.error(
          actual,
          new IllegalStateException("UnicastMonoProcessor allows only a single Subscriber"));
    }
  }

  @Override
  public final void request(long n) {
    if (Operators.validate(n)) {
      for (; ; ) {
        int s = state;
        // if the any bits 1-31 are set, we are either in fusion mode (FUSED_*)
        // or request has been called (HAS_REQUEST_*)
        if ((s & ~NO_REQUEST_HAS_RESULT) != 0) {
          return;
        }
        if (s == NO_REQUEST_HAS_RESULT) {
          if (STATE.compareAndSet(this, NO_REQUEST_HAS_RESULT, HAS_REQUEST_HAS_RESULT)) {
            final Subscriber<? super O> a = actual;
            final O v = value;
            actual = null;
            value = null;
            lifecycleHandler.doOnTerminal(SignalType.ON_COMPLETE, v, null);
            a.onNext(v);
            a.onComplete();
            return;
          }
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
      value = null;
      actual = null;
      lifecycleHandler.doOnTerminal(SignalType.CANCEL, null, null);
      final Subscription s = UPSTREAM.getAndSet(this, Operators.cancelledSubscription());
      if (s != null && s != Operators.cancelledSubscription()) {
        s.cancel();
      }
    }
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
    if (isCancelled()) {
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
   * Indicates whether this {@code UnicastMonoProcessor} has been completed with an error.
   *
   * @return {@code true} if this {@code UnicastMonoProcessor} was completed with an error, {@code
   *     false} otherwise.
   */
  public final boolean isError() {
    return getError() != null;
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

  public final boolean isTerminated() {
    int state = this.state;
    return (state < CANCELLED && state % 2 == 1);
  }

  @Override
  public boolean isDisposed() {
    int state = this.state;
    return state == CANCELLED || (state < CANCELLED && state % 2 == 1);
  }

  @Override
  @Nullable
  public Object scanUnsafe(Attr key) {
    // touch guard
    int state = this.state;

    if (key == Attr.TERMINATED) {
      return (state < CANCELLED && state % 2 == 1);
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
