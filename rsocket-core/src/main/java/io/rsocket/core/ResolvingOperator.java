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
package io.rsocket.core;

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

// A copy of this class exists in io.rsocket.loadbalance

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

  abstract static class DeferredResolution<T, R>
      implements CoreSubscriber<T>, Subscription, Scannable, BiConsumer<R, Throwable> {

    final ResolvingOperator<R> parent;
    final CoreSubscriber<? super T> actual;

    volatile long requested;

    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<DeferredResolution> REQUESTED =
        AtomicLongFieldUpdater.newUpdater(DeferredResolution.class, "requested");

    static final long STATE_SUBSCRIBED = -1;
    static final long STATE_CANCELLED = Long.MIN_VALUE;

    Subscription s;
    boolean done;

    DeferredResolution(ResolvingOperator<R> parent, CoreSubscriber<? super T> actual) {
      this.parent = parent;
      this.actual = actual;
    }

    @Override
    public final Context currentContext() {
      return this.actual.currentContext();
    }

    @Nullable
    @Override
    public Object scanUnsafe(Attr key) {
      long state = this.requested;

      if (key == Attr.PARENT) {
        return this.s;
      }
      if (key == Attr.ACTUAL) {
        return this.parent;
      }
      if (key == Attr.TERMINATED) {
        return this.done;
      }
      if (key == Attr.CANCELLED) {
        return state == STATE_CANCELLED;
      }

      return null;
    }

    @Override
    public final void onSubscribe(Subscription s) {
      final long state = this.requested;
      Subscription a = this.s;
      if (state == STATE_CANCELLED) {
        s.cancel();
        return;
      }
      if (a != null) {
        s.cancel();
        return;
      }

      long r;
      long accumulated = 0;
      for (; ; ) {
        r = this.requested;

        if (r == STATE_CANCELLED || r == STATE_SUBSCRIBED) {
          s.cancel();
          return;
        }

        this.s = s;

        long toRequest = r - accumulated;
        if (toRequest > 0) { // if there is something,
          s.request(toRequest); // then we do a request on the given subscription
        }
        accumulated = r;

        if (REQUESTED.compareAndSet(this, r, STATE_SUBSCRIBED)) {
          return;
        }
      }
    }

    @Override
    public final void onNext(T payload) {
      this.actual.onNext(payload);
    }

    @Override
    public final void onError(Throwable t) {
      if (this.done) {
        Operators.onErrorDropped(t, this.actual.currentContext());
        return;
      }

      this.done = true;
      this.actual.onError(t);
    }

    @Override
    public final void onComplete() {
      if (this.done) {
        return;
      }

      this.done = true;
      this.actual.onComplete();
    }

    @Override
    public void request(long n) {
      if (Operators.validate(n)) {
        long r = this.requested; // volatile read beforehand

        if (r > STATE_SUBSCRIBED) { // works only in case onSubscribe has not happened
          long u;
          for (; ; ) { // normal CAS loop with overflow protection
            if (r == Long.MAX_VALUE) {
              // if r == Long.MAX_VALUE then we dont care and we can loose this
              // request just in case of racing
              return;
            }
            u = Operators.addCap(r, n);
            if (REQUESTED.compareAndSet(this, r, u)) {
              // Means increment happened before onSubscribe
              return;
            } else {
              // Means increment happened after onSubscribe

              // update new state to see what exactly happened (onSubscribe |cancel | requestN)
              r = this.requested;

              // check state (expect -1 | -2 to exit, otherwise repeat)
              if (r < 0) {
                break;
              }
            }
          }
        }

        if (r == STATE_CANCELLED) { // if canceled, just exit
          return;
        }

        // if onSubscribe -> subscription exists (and we sure of that because volatile read
        // after volatile write) so we can execute requestN on the subscription
        this.s.request(n);
      }
    }

    public boolean isCancelled() {
      return this.requested == STATE_CANCELLED;
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
      }
    }
  }

  static class MonoDeferredResolutionOperator<T> extends Operators.MonoSubscriber<T, T>
      implements BiConsumer<T, Throwable> {

    final ResolvingOperator<T> parent;

    MonoDeferredResolutionOperator(ResolvingOperator<T> parent, CoreSubscriber<? super T> actual) {
      super(actual);
      this.parent = parent;
    }

    @Override
    public void accept(T t, Throwable throwable) {
      if (throwable != null) {
        onError(throwable);
        return;
      }

      complete(t);
    }

    @Override
    public void cancel() {
      if (!isCancelled()) {
        super.cancel();
        this.parent.remove(this);
      }
    }

    @Override
    public void onComplete() {
      if (!isCancelled()) {
        this.actual.onComplete();
      }
    }

    @Override
    public void onError(Throwable t) {
      if (isCancelled()) {
        Operators.onErrorDropped(t, currentContext());
      } else {
        this.actual.onError(t);
      }
    }

    @Override
    public Object scanUnsafe(Attr key) {
      if (key == Attr.PARENT) return this.parent;
      return super.scanUnsafe(key);
    }
  }
}
