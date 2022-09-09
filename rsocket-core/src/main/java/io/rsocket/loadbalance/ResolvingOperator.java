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

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

// This class is a copy of the same class in io.rsocket.core

class ResolvingOperator<T> implements Disposable {

  static final CancellationException ON_DISPOSE = new CancellationException("Disposed");

  volatile int wip;

  @SuppressWarnings("rawtypes")
  static final AtomicIntegerFieldUpdater<ResolvingOperator> WIP =
      AtomicIntegerFieldUpdater.newUpdater(ResolvingOperator.class, "wip");

  volatile BiConsumer<T, Throwable>[] subscribers;

  @SuppressWarnings("rawtypes")
  static final AtomicReferenceFieldUpdater<ResolvingOperator, BiConsumer[]> SUBSCRIBERS =
      AtomicReferenceFieldUpdater.newUpdater(
          ResolvingOperator.class, BiConsumer[].class, "subscribers");

  @SuppressWarnings("unchecked")
  static final BiConsumer<?, Throwable>[] EMPTY_UNSUBSCRIBED = new BiConsumer[0];

  @SuppressWarnings("unchecked")
  static final BiConsumer<?, Throwable>[] EMPTY_SUBSCRIBED = new BiConsumer[0];

  @SuppressWarnings("unchecked")
  static final BiConsumer<?, Throwable>[] READY = new BiConsumer[0];

  @SuppressWarnings("unchecked")
  static final BiConsumer<?, Throwable>[] TERMINATED = new BiConsumer[0];

  static final int ADDED_STATE = 0;
  static final int READY_STATE = 1;
  static final int TERMINATED_STATE = 2;

  T value;
  Throwable t;

  public ResolvingOperator() {

    SUBSCRIBERS.lazySet(this, EMPTY_UNSUBSCRIBED);
  }

  @Override
  public final void dispose() {
    this.terminate(ON_DISPOSE);
  }

  @Override
  public final boolean isDisposed() {
    return this.subscribers == TERMINATED;
  }

  public final boolean isPending() {
    BiConsumer<T, Throwable>[] state = this.subscribers;
    return state != READY && state != TERMINATED;
  }

  @Nullable
  public final T valueIfResolved() {
    if (this.subscribers == READY) {
      T value = this.value;
      if (value != null) {
        return value;
      }
    }

    return null;
  }

  final void observe(BiConsumer<T, Throwable> actual) {
    for (; ; ) {
      final int state = this.add(actual);

      T value = this.value;

      if (state == READY_STATE) {
        if (value != null) {
          actual.accept(value, null);
          return;
        }
        // value == null means racing between invalidate and this subscriber
        // thus, we have to loop again
        continue;
      } else if (state == TERMINATED_STATE) {
        actual.accept(null, this.t);
        return;
      }

      return;
    }
  }

  /**
   * Block the calling thread for the specified time, waiting for the completion of this {@code
   * ReconnectMono}. If the {@link ResolvingOperator} is completed with an error a RuntimeException
   * that wraps the error is thrown.
   *
   * @param timeout the timeout value as a {@link Duration}
   * @return the value of this {@link ResolvingOperator} or {@code null} if the timeout is reached
   *     and the {@link ResolvingOperator} has not completed
   * @throws RuntimeException if terminated with error
   * @throws IllegalStateException if timed out or {@link Thread} was interrupted with {@link
   *     InterruptedException}
   */
  @Nullable
  @SuppressWarnings({"uncheked", "BusyWait"})
  public T block(@Nullable Duration timeout) {
    try {
      BiConsumer<T, Throwable>[] subscribers = this.subscribers;
      if (subscribers == READY) {
        final T value = this.value;
        if (value != null) {
          return value;
        } else {
          // value == null means racing between invalidate and this block
          // thus, we have to update the state again and see what happened
          subscribers = this.subscribers;
        }
      }

      if (subscribers == TERMINATED) {
        RuntimeException re = Exceptions.propagate(this.t);
        re = Exceptions.addSuppressed(re, new Exception("Terminated with an error"));
        throw re;
      }

      // connect once
      if (subscribers == EMPTY_UNSUBSCRIBED
          && SUBSCRIBERS.compareAndSet(this, EMPTY_UNSUBSCRIBED, EMPTY_SUBSCRIBED)) {
        this.doSubscribe();
      }

      long delay;
      if (null == timeout) {
        delay = 0L;
      } else {
        delay = System.nanoTime() + timeout.toNanos();
      }
      for (; ; ) {
        subscribers = this.subscribers;

        if (subscribers == READY) {
          final T value = this.value;
          if (value != null) {
            return value;
          } else {
            // value == null means racing between invalidate and this block
            // thus, we have to update the state again and see what happened
            subscribers = this.subscribers;
          }
        }
        if (subscribers == TERMINATED) {
          RuntimeException re = Exceptions.propagate(this.t);
          re = Exceptions.addSuppressed(re, new Exception("Terminated with an error"));
          throw re;
        }
        if (timeout != null && delay < System.nanoTime()) {
          throw new IllegalStateException("Timeout on Mono blocking read");
        }

        // connect again since invalidate() has happened in between
        if (subscribers == EMPTY_UNSUBSCRIBED
            && SUBSCRIBERS.compareAndSet(this, EMPTY_UNSUBSCRIBED, EMPTY_SUBSCRIBED)) {
          this.doSubscribe();
        }

        Thread.sleep(1);
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();

      throw new IllegalStateException("Thread Interruption on Mono blocking read");
    }
  }

  @SuppressWarnings("unchecked")
  final void terminate(Throwable t) {
    if (isDisposed()) {
      Operators.onErrorDropped(t, Context.empty());
      return;
    }

    // writes happens before volatile write
    this.t = t;

    final BiConsumer<T, Throwable>[] subscribers = SUBSCRIBERS.getAndSet(this, TERMINATED);
    if (subscribers == TERMINATED) {
      Operators.onErrorDropped(t, Context.empty());
      return;
    }

    this.doOnDispose();

    this.doFinally();

    for (BiConsumer<T, Throwable> consumer : subscribers) {
      consumer.accept(null, t);
    }
  }

  final void complete(T value) {
    BiConsumer<T, Throwable>[] subscribers = this.subscribers;
    if (subscribers == TERMINATED) {
      this.doOnValueExpired(value);
      return;
    }

    this.value = value;

    for (; ; ) {
      // ensures TERMINATE is going to be replaced with READY
      if (SUBSCRIBERS.compareAndSet(this, subscribers, READY)) {
        break;
      }

      subscribers = this.subscribers;

      if (subscribers == TERMINATED) {
        this.doFinally();
        return;
      }
    }

    this.doOnValueResolved(value);

    for (BiConsumer<T, Throwable> consumer : subscribers) {
      consumer.accept(value, null);
    }
  }

  protected void doOnValueResolved(T value) {
    // no ops
  }

  final void doFinally() {
    if (WIP.getAndIncrement(this) != 0) {
      return;
    }

    int m = 1;
    T value;

    for (; ; ) {
      value = this.value;
      if (value != null && isDisposed()) {
        this.value = null;
        this.doOnValueExpired(value);
        return;
      }

      m = WIP.addAndGet(this, -m);
      if (m == 0) {
        return;
      }
    }
  }

  final void invalidate() {
    if (this.subscribers == TERMINATED) {
      return;
    }

    final BiConsumer<T, Throwable>[] subscribers = this.subscribers;

    if (subscribers == READY) {
      // guarded section to ensure we expire value exactly once if there is racing
      if (WIP.getAndIncrement(this) != 0) {
        return;
      }

      final T value = this.value;
      if (value != null) {
        this.value = null;
        this.doOnValueExpired(value);
      }

      int m = 1;
      for (; ; ) {
        if (isDisposed()) {
          return;
        }

        m = WIP.addAndGet(this, -m);
        if (m == 0) {
          break;
        }
      }

      SUBSCRIBERS.compareAndSet(this, READY, EMPTY_UNSUBSCRIBED);
    }
  }

  protected void doOnValueExpired(T value) {
    // no ops
  }

  protected void doOnDispose() {
    // no ops
  }

  public final boolean connect() {
    for (; ; ) {
      final BiConsumer<T, Throwable>[] a = this.subscribers;

      if (a == TERMINATED) {
        return false;
      }

      if (a == READY) {
        return true;
      }

      if (a != EMPTY_UNSUBSCRIBED) {
        // do nothing if already started
        return true;
      }

      if (SUBSCRIBERS.compareAndSet(this, a, EMPTY_SUBSCRIBED)) {
        this.doSubscribe();
        return true;
      }
    }
  }

  final int add(BiConsumer<T, Throwable> ps) {
    for (; ; ) {
      BiConsumer<T, Throwable>[] a = this.subscribers;

      if (a == TERMINATED) {
        return TERMINATED_STATE;
      }

      if (a == READY) {
        return READY_STATE;
      }

      int n = a.length;
      @SuppressWarnings("unchecked")
      BiConsumer<T, Throwable>[] b = new BiConsumer[n + 1];
      System.arraycopy(a, 0, b, 0, n);
      b[n] = ps;

      if (SUBSCRIBERS.compareAndSet(this, a, b)) {
        if (a == EMPTY_UNSUBSCRIBED) {
          this.doSubscribe();
        }
        return ADDED_STATE;
      }
    }
  }

  protected void doSubscribe() {
    // no ops
  }

  @SuppressWarnings("unchecked")
  final void remove(BiConsumer<T, Throwable> ps) {
    for (; ; ) {
      BiConsumer<T, Throwable>[] a = this.subscribers;
      int n = a.length;
      if (n == 0) {
        return;
      }

      int j = -1;
      for (int i = 0; i < n; i++) {
        if (a[i] == ps) {
          j = i;
          break;
        }
      }

      if (j < 0) {
        return;
      }

      BiConsumer<?, Throwable>[] b;

      if (n == 1) {
        b = EMPTY_SUBSCRIBED;
      } else {
        b = new BiConsumer[n - 1];
        System.arraycopy(a, 0, b, 0, j);
        System.arraycopy(a, j + 1, b, j, n - j - 1);
      }
      if (SUBSCRIBERS.compareAndSet(this, a, b)) {
        return;
      }
    }
  }
}
