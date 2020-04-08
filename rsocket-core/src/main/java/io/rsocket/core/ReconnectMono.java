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

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Operators.MonoSubscriber;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

final class ReconnectMono<T> extends Mono<T> implements Invalidatable, Disposable, Scannable {

  final Mono<T> source;
  final BiConsumer<? super T, Invalidatable> onValueReceived;
  final Consumer<? super T> onValueExpired;
  final ReconnectMainSubscriber<? super T> mainSubscriber;

  volatile int wip;

  @SuppressWarnings("rawtypes")
  static final AtomicIntegerFieldUpdater<ReconnectMono> WIP =
      AtomicIntegerFieldUpdater.newUpdater(ReconnectMono.class, "wip");

  volatile ReconnectInner<T>[] subscribers;

  @SuppressWarnings("rawtypes")
  static final AtomicReferenceFieldUpdater<ReconnectMono, ReconnectInner[]> SUBSCRIBERS =
      AtomicReferenceFieldUpdater.newUpdater(
          ReconnectMono.class, ReconnectInner[].class, "subscribers");

  @SuppressWarnings("rawtypes")
  static final ReconnectInner[] EMPTY_UNSUBSCRIBED = new ReconnectInner[0];

  @SuppressWarnings("rawtypes")
  static final ReconnectInner[] EMPTY_SUBSCRIBED = new ReconnectInner[0];

  @SuppressWarnings("rawtypes")
  static final ReconnectInner[] READY = new ReconnectInner[0];

  @SuppressWarnings("rawtypes")
  static final ReconnectInner[] TERMINATED = new ReconnectInner[0];

  static final int ADDED_STATE = 0;
  static final int READY_STATE = 1;
  static final int TERMINATED_STATE = 2;

  T value;
  Throwable t;

  ReconnectMono(
      Mono<T> source,
      Consumer<? super T> onValueExpired,
      BiConsumer<? super T, Invalidatable> onValueReceived) {
    this.source = source;
    this.onValueExpired = onValueExpired;
    this.onValueReceived = onValueReceived;
    this.mainSubscriber = new ReconnectMainSubscriber<>(this);

    SUBSCRIBERS.lazySet(this, EMPTY_UNSUBSCRIBED);
  }

  @Override
  public Object scanUnsafe(Attr key) {
    if (key == Attr.PARENT) return source;
    if (key == Attr.PREFETCH) return Integer.MAX_VALUE;

    final boolean isDisposed = isDisposed();
    if (key == Attr.TERMINATED) return isDisposed;
    if (key == Attr.ERROR) return t;

    return null;
  }

  @Override
  public void dispose() {
    this.terminate(new CancellationException("ReconnectMono has already been disposed"));
  }

  @Override
  public boolean isDisposed() {
    return this.subscribers == TERMINATED;
  }

  @Override
  @SuppressWarnings("uncheked")
  public void subscribe(CoreSubscriber<? super T> actual) {
    final ReconnectInner<T> inner = new ReconnectInner<>(actual, this);
    actual.onSubscribe(inner);

    final int state = this.add(inner);

    if (state == READY_STATE) {
      inner.complete(this.value);
    } else if (state == TERMINATED_STATE) {
      inner.onError(this.t);
    }
  }

  /**
   * Block the calling thread indefinitely, waiting for the completion of this {@code
   * ReconnectMono}. If the {@link ReconnectMono} is completed with an error a RuntimeException that
   * wraps the error is thrown.
   *
   * @return the value of this {@code ReconnectMono}
   */
  @Override
  @Nullable
  public T block() {
    return block(null);
  }

  /**
   * Block the calling thread for the specified time, waiting for the completion of this {@code
   * ReconnectMono}. If the {@link ReconnectMono} is completed with an error a RuntimeException that
   * wraps the error is thrown.
   *
   * @param timeout the timeout value as a {@link Duration}
   * @return the value of this {@code ReconnectMono} or {@code null} if the timeout is reached and
   *     the {@code ReconnectMono} has not completed
   */
  @Override
  @Nullable
  @SuppressWarnings("uncheked")
  public T block(@Nullable Duration timeout) {
    try {
      ReconnectInner<T>[] subscribers = this.subscribers;
      if (subscribers == READY) {
        return this.value;
      }

      if (subscribers == TERMINATED) {
        RuntimeException re = Exceptions.propagate(this.t);
        re = Exceptions.addSuppressed(re, new Exception("ReconnectMono terminated with an error"));
        throw re;
      }

      // connect once
      if (subscribers == EMPTY_UNSUBSCRIBED
          && SUBSCRIBERS.compareAndSet(this, EMPTY_UNSUBSCRIBED, EMPTY_SUBSCRIBED)) {
        this.source.subscribe(this.mainSubscriber);
      }

      long delay;
      if (null == timeout) {
        delay = 0L;
      } else {
        delay = System.nanoTime() + timeout.toNanos();
      }
      for (; ; ) {
        ReconnectInner<T>[] inners = this.subscribers;

        if (inners == READY) {
          return this.value;
        }
        if (inners == TERMINATED) {
          RuntimeException re = Exceptions.propagate(this.t);
          re =
              Exceptions.addSuppressed(re, new Exception("ReconnectMono terminated with an error"));
          throw re;
        }
        if (timeout != null && delay < System.nanoTime()) {
          throw new IllegalStateException("Timeout on Mono blocking read");
        }

        Thread.sleep(1);
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();

      throw new IllegalStateException("Thread Interruption on Mono blocking read");
    }
  }

  @SuppressWarnings("unchecked")
  void terminate(Throwable t) {
    if (isDisposed()) {
      return;
    }

    // writes happens before volatile write
    this.t = t;

    final ReconnectInner<T>[] subscribers = SUBSCRIBERS.getAndSet(this, TERMINATED);
    if (subscribers == TERMINATED) {
      Operators.onErrorDropped(t, Context.empty());
      return;
    }

    this.mainSubscriber.dispose();

    this.doFinally();

    for (CoreSubscriber<T> consumer : subscribers) {
      consumer.onError(t);
    }
  }

  void complete() {
    ReconnectInner<T>[] subscribers = this.subscribers;
    if (subscribers == TERMINATED) {
      return;
    }

    final T value = this.value;

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

    this.onValueReceived.accept(value, this);

    for (ReconnectInner<T> consumer : subscribers) {
      consumer.complete(value);
    }
  }

  void doFinally() {
    if (WIP.getAndIncrement(this) != 0) {
      return;
    }

    int m = 1;
    T value;

    for (; ; ) {
      value = this.value;

      if (value != null && isDisposed()) {
        this.value = null;
        this.onValueExpired.accept(value);
        return;
      }

      m = WIP.addAndGet(this, -m);
      if (m == 0) {
        return;
      }
    }
  }

  // Check RSocket is not good
  @Override
  public void invalidate() {
    if (this.subscribers == TERMINATED) {
      return;
    }

    final ReconnectInner<T>[] subscribers = this.subscribers;

    if (subscribers == READY && SUBSCRIBERS.compareAndSet(this, READY, EMPTY_UNSUBSCRIBED)) {
      final T value = this.value;
      this.value = null;

      if (value != null) {
        this.onValueExpired.accept(value);
      }
    }
  }

  int add(ReconnectInner<T> ps) {
    for (; ; ) {
      ReconnectInner<T>[] a = this.subscribers;

      if (a == TERMINATED) {
        return TERMINATED_STATE;
      }

      if (a == READY) {
        return READY_STATE;
      }

      int n = a.length;
      @SuppressWarnings("unchecked")
      ReconnectInner<T>[] b = new ReconnectInner[n + 1];
      System.arraycopy(a, 0, b, 0, n);
      b[n] = ps;

      if (SUBSCRIBERS.compareAndSet(this, a, b)) {
        if (a == EMPTY_UNSUBSCRIBED) {
          this.source.subscribe(this.mainSubscriber);
        }
        return ADDED_STATE;
      }
    }
  }

  @SuppressWarnings("unchecked")
  void remove(ReconnectInner<T> ps) {
    for (; ; ) {
      ReconnectInner<T>[] a = this.subscribers;
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

      ReconnectInner<T>[] b;

      if (n == 1) {
        b = EMPTY_SUBSCRIBED;
      } else {
        b = new ReconnectInner[n - 1];
        System.arraycopy(a, 0, b, 0, j);
        System.arraycopy(a, j + 1, b, j, n - j - 1);
      }
      if (SUBSCRIBERS.compareAndSet(this, a, b)) {
        return;
      }
    }
  }

  static final class ReconnectMainSubscriber<T> implements CoreSubscriber<T> {

    final ReconnectMono<T> parent;

    volatile Subscription s;

    @SuppressWarnings("rawtypes")
    static final AtomicReferenceFieldUpdater<ReconnectMainSubscriber, Subscription> S =
        AtomicReferenceFieldUpdater.newUpdater(
            ReconnectMainSubscriber.class, Subscription.class, "s");

    ReconnectMainSubscriber(ReconnectMono<T> parent) {
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
      final ReconnectMono<T> p = this.parent;
      final T value = p.value;

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
      final ReconnectMono<T> p = this.parent;

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
    public void onNext(T value) {
      if (this.s == Operators.cancelledSubscription()) {
        this.parent.onValueExpired.accept(value);
        return;
      }

      final ReconnectMono<T> p = this.parent;

      p.value = value;
      // volatile write and check on racing
      p.doFinally();
    }

    void dispose() {
      Operators.terminate(S, this);
    }
  }

  static final class ReconnectInner<T> extends MonoSubscriber<T, T> {
    final ReconnectMono<T> parent;

    ReconnectInner(CoreSubscriber<? super T> actual, ReconnectMono<T> parent) {
      super(actual);
      this.parent = parent;
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

interface Invalidatable {

  void invalidate();
}
