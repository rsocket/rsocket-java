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
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

final class ReconnectMono<T> extends Mono<T> implements Invalidatable, Disposable, Scannable {

  final Mono<T> source;
  final BiConsumer<? super T, Invalidatable> onValueReceived;
  final Consumer<? super T> onValueExpired;
  final ResolvingInner<T> resolvingInner;

  ReconnectMono(
      Mono<T> source,
      Consumer<? super T> onValueExpired,
      BiConsumer<? super T, Invalidatable> onValueReceived) {
    this.source = source;
    this.onValueExpired = onValueExpired;
    this.onValueReceived = onValueReceived;
    this.resolvingInner = new ResolvingInner<>(this);
  }

  public Mono<T> getSource() {
    return source;
  }

  @Override
  public Object scanUnsafe(Attr key) {
    if (key == Attr.PARENT) return source;
    if (key == Attr.PREFETCH) return Integer.MAX_VALUE;

    final boolean isDisposed = isDisposed();
    if (key == Attr.TERMINATED) return isDisposed;
    if (key == Attr.ERROR) return this.resolvingInner.t;

    return null;
  }

  @Override
  public void invalidate() {
    this.resolvingInner.invalidate();
  }

  @Override
  public void dispose() {
    this.resolvingInner.terminate(
        new CancellationException("ReconnectMono has already been disposed"));
  }

  @Override
  public boolean isDisposed() {
    return this.resolvingInner.isDisposed();
  }

  @Override
  @SuppressWarnings("uncheked")
  public void subscribe(CoreSubscriber<? super T> actual) {
    final ResolvingOperator.MonoDeferredResolutionOperator<T> inner =
        new ResolvingOperator.MonoDeferredResolutionOperator<>(this.resolvingInner, actual);
    actual.onSubscribe(inner);

    this.resolvingInner.observe(inner);
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
    return this.resolvingInner.block(timeout);
  }

  /**
   * Subscriber that subscribes to the source {@link Mono} to receive its value. <br>
   * Note that the source is not expected to complete empty, and if this happens, execution will
   * terminate with an {@code IllegalStateException}.
   */
  static final class ReconnectMainSubscriber<T> implements CoreSubscriber<T> {

    final ResolvingInner<T> parent;

    volatile Subscription s;

    @SuppressWarnings("rawtypes")
    static final AtomicReferenceFieldUpdater<ReconnectMainSubscriber, Subscription> S =
        AtomicReferenceFieldUpdater.newUpdater(
            ReconnectMainSubscriber.class, Subscription.class, "s");

    volatile int wip;

    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<ReconnectMainSubscriber> WIP =
        AtomicIntegerFieldUpdater.newUpdater(ReconnectMainSubscriber.class, "wip");

    T value;

    ReconnectMainSubscriber(ResolvingInner<T> parent) {
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
      final T value = this.value;

      if (s == Operators.cancelledSubscription() || !S.compareAndSet(this, s, null)) {
        this.doFinally();
        return;
      }

      final ResolvingInner<T> p = this.parent;
      if (value == null) {
        p.terminate(new IllegalStateException("Source completed empty"));
      } else {
        p.complete(value);
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
      this.parent.terminate(t);
    }

    @Override
    public void onNext(T value) {
      if (this.s == Operators.cancelledSubscription()) {
        this.parent.doOnValueExpired(value);
        return;
      }

      this.value = value;
      // volatile write and check on racing
      this.doFinally();
    }

    void dispose() {
      if (Operators.terminate(S, this)) {
        this.doFinally();
      }
    }

    final void doFinally() {
      if (WIP.getAndIncrement(this) != 0) {
        return;
      }

      int m = 1;
      T value;

      for (; ; ) {
        value = this.value;
        if (value != null && this.s == Operators.cancelledSubscription()) {
          this.value = null;
          this.parent.doOnValueExpired(value);
          return;
        }

        m = WIP.addAndGet(this, -m);
        if (m == 0) {
          return;
        }
      }
    }
  }

  static final class ResolvingInner<T> extends ResolvingOperator<T> implements Scannable {

    final ReconnectMono<T> parent;
    final ReconnectMainSubscriber<? super T> mainSubscriber;

    ResolvingInner(ReconnectMono<T> parent) {
      this.parent = parent;
      this.mainSubscriber = new ReconnectMainSubscriber<>(this);
    }

    @Override
    protected void doOnValueExpired(T value) {
      this.parent.onValueExpired.accept(value);
    }

    @Override
    protected void doOnValueResolved(T value) {
      this.parent.onValueReceived.accept(value, this.parent);
    }

    @Override
    protected void doOnDispose() {
      this.mainSubscriber.dispose();
    }

    @Override
    protected void doSubscribe() {
      this.parent.source.subscribe(this.mainSubscriber);
    }

    @Override
    public Object scanUnsafe(Attr key) {
      if (key == Attr.PARENT) return this.parent;
      return null;
    }
  }
}

interface Invalidatable {

  void invalidate();
}
