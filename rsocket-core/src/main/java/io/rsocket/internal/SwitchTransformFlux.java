/*
 * Copyright 2015-2018 the original author or authors.
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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;

import io.netty.util.ReferenceCountUtil;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;

public final class SwitchTransformFlux<T, R> extends Flux<R> {

  final Publisher<? extends T> source;
  final BiFunction<T, Flux<T>, Publisher<? extends R>> transformer;

  public SwitchTransformFlux(
          Publisher<? extends T> source, BiFunction<T, Flux<T>, Publisher<? extends R>> transformer) {
    this.source = Objects.requireNonNull(source, "source");
    this.transformer = Objects.requireNonNull(transformer, "transformer");
  }

  @Override
  public int getPrefetch() {
    return 1;
  }

  @Override
  public void subscribe(CoreSubscriber<? super R> actual) {
    source.subscribe(new SwitchTransformMain<>(actual, transformer));
  }

  static final class SwitchTransformMain<T, R> implements CoreSubscriber<T>, Scannable {

    final CoreSubscriber<? super R> actual;
    final BiFunction<T, Flux<T>, Publisher<? extends R>> transformer;
    final SwitchTransformInner<T> inner;

    Subscription s;

    volatile int once;
    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<SwitchTransformMain> ONCE =
            AtomicIntegerFieldUpdater.newUpdater(SwitchTransformMain.class, "once");

    SwitchTransformMain(
            CoreSubscriber<? super R> actual,
            BiFunction<T, Flux<T>, Publisher<? extends R>> transformer
    ) {
      this.actual = actual;
      this.transformer = transformer;
      this.inner = new SwitchTransformInner<>(this);
    }

    @Override
    @Nullable
    public Object scanUnsafe(Attr key) {
      if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();
      if (key == Attr.PREFETCH) return 1;

      return null;
    }

    @Override
    public void onSubscribe(Subscription s) {
      if (Operators.validate(this.s, s)) {
        this.s = s;
        s.request(1);
      }
    }

    @Override
    public void onNext(T t) {
      if (isCanceled()) {
        return;
      }

      if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
        try {
          inner.first = t;
          Publisher<? extends R> result =
            Objects.requireNonNull(transformer.apply(t, inner), "The transformer returned a null value");
          result.subscribe(actual);
          return;
        } catch (Throwable e) {
          onError(Operators.onOperatorError(s, e, t, actual.currentContext()));
          ReferenceCountUtil.safeRelease(t);
          return;
        }
      }

      inner.onNext(t);
    }

    @Override
    public void onError(Throwable t) {
      if (isCanceled()) {
        return;
      }

      if (once != 0) {
        inner.onError(t);
      } else {
        actual.onSubscribe(Operators.emptySubscription());
        actual.onError(t);
      }
    }

    @Override
    public void onComplete() {
      if (isCanceled()) {
        return;
      }

      if (once != 0) {
        inner.onComplete();
      } else {
        actual.onSubscribe(Operators.emptySubscription());
        actual.onComplete();
      }
    }

    boolean isCanceled() {
      return s == Operators.cancelledSubscription();
    }

    void cancel() {
      s.cancel();
      s = Operators.cancelledSubscription();
    }
  }

  static final class SwitchTransformInner<V> extends Flux<V>
          implements Scannable, Subscription {

    final SwitchTransformMain<V, ?> parent;

    volatile CoreSubscriber<? super V> actual;
    @SuppressWarnings("rawtypes")
    static final AtomicReferenceFieldUpdater<SwitchTransformInner, CoreSubscriber> ACTUAL =
            AtomicReferenceFieldUpdater.newUpdater(SwitchTransformInner.class, CoreSubscriber.class, "actual");

    volatile V first;
    @SuppressWarnings("rawtypes")
    static final AtomicReferenceFieldUpdater<SwitchTransformInner, Object> FIRST =
            AtomicReferenceFieldUpdater.newUpdater(SwitchTransformInner.class, Object.class, "first");

    volatile int once;
    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<SwitchTransformInner> ONCE =
            AtomicIntegerFieldUpdater.newUpdater(SwitchTransformInner.class, "once");

    SwitchTransformInner(SwitchTransformMain<V, ?> parent) {
      this.parent = parent;
    }

    public void onNext(V t) {
      CoreSubscriber<? super V> a = actual;

      if (a != null) {
        a.onNext(t);
      }
    }

    public void onError(Throwable t) {
      CoreSubscriber<? super V> a = actual;

      if (a != null) {
        a.onError(t);
      }
    }

    public void onComplete() {
      CoreSubscriber<? super V> a = actual;

      if (a != null) {
        a.onComplete();
      }
    }

    @Override
    public void subscribe(CoreSubscriber<? super V> actual) {
      if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
        ACTUAL.lazySet(this, actual);
        actual.onSubscribe(this);
      }
      else {
        actual.onError(new IllegalStateException("SwitchTransform allows only one Subscriber"));
      }
    }

    @Override
    public void request(long n) {
      V f = first;

      if (f != null && FIRST.compareAndSet(this, f, null)) {
        actual.onNext(f);

        long r = Operators.addCap(n, -1);
        if (r > 0) {
          parent.s.request(r);
        }
      } else {
        parent.s.request(n);
      }
    }

    @Override
    public void cancel() {
      actual = null;
      first = null;
      parent.cancel();
    }

    @Override
    @Nullable
    public Object scanUnsafe(Attr key) {
      if (key == Attr.PARENT) return parent;
      if (key == Attr.ACTUAL) return actual();

      return null;
    }

    public CoreSubscriber<? super V> actual() {
      return actual;
    }
  }
}