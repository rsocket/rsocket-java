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
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

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
  @SuppressWarnings("unchecked")
  public void subscribe(CoreSubscriber<? super R> actual) {
    if (actual instanceof Fuseable.ConditionalSubscriber) {
      source.subscribe(
          new SwitchTransformConditionalOperator<>(
              (Fuseable.ConditionalSubscriber<? super R>) actual, transformer));
      return;
    }
    source.subscribe(new SwitchTransformOperator<>(actual, transformer));
  }

  static final class SwitchTransformOperator<T, R> extends Flux<T>
      implements CoreSubscriber<T>, Subscription, Scannable {

    final CoreSubscriber<? super R> outer;
    final BiFunction<T, Flux<T>, Publisher<? extends R>> transformer;

    Subscription s;
    Throwable throwable;

    volatile boolean done;
    volatile T first;

    volatile CoreSubscriber<? super T> inner;

    @SuppressWarnings("rawtypes")
    static final AtomicReferenceFieldUpdater<SwitchTransformOperator, CoreSubscriber> INNER =
        AtomicReferenceFieldUpdater.newUpdater(
            SwitchTransformOperator.class, CoreSubscriber.class, "inner");

    volatile int wip;

    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<SwitchTransformOperator> WIP =
        AtomicIntegerFieldUpdater.newUpdater(SwitchTransformOperator.class, "wip");

    volatile int once;

    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<SwitchTransformOperator> ONCE =
        AtomicIntegerFieldUpdater.newUpdater(SwitchTransformOperator.class, "once");

    SwitchTransformOperator(
        CoreSubscriber<? super R> outer,
        BiFunction<T, Flux<T>, Publisher<? extends R>> transformer) {
      this.outer = outer;
      this.transformer = transformer;
    }

    @Override
    @Nullable
    public Object scanUnsafe(Attr key) {
      if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();
      if (key == Attr.PREFETCH) return 1;

      return null;
    }

    @Override
    public Context currentContext() {
      CoreSubscriber<? super T> actual = inner;

      if (actual != null) {
        return actual.currentContext();
      }

      return outer.currentContext();
    }

    @Override
    public void cancel() {
      if (s != Operators.cancelledSubscription()) {
        Subscription s = this.s;
        this.s = Operators.cancelledSubscription();

        if (WIP.getAndIncrement(this) == 0) {
          INNER.lazySet(this, null);

          T f = first;
          if (f != null) {
            first = null;
            Operators.onDiscard(f, currentContext());
          }
        }

        s.cancel();
      }
    }

    @Override
    public void subscribe(CoreSubscriber<? super T> actual) {
      if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
        INNER.lazySet(this, actual);
        actual.onSubscribe(this);
      } else {
        Operators.error(
            actual, new IllegalStateException("SwitchTransform allows only one Subscriber"));
      }
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
      if (done) {
        Operators.onNextDropped(t, currentContext());
        return;
      }

      CoreSubscriber<? super T> i = inner;

      if (i == null) {
        try {
          first = t;
          Publisher<? extends R> result =
              Objects.requireNonNull(
                  transformer.apply(t, this), "The transformer returned a null value");
          result.subscribe(outer);
          return;
        } catch (Throwable e) {
          onError(Operators.onOperatorError(s, e, t, currentContext()));
          return;
        }
      }

      i.onNext(t);
    }

    @Override
    public void onError(Throwable t) {
      if (done) {
        Operators.onErrorDropped(t, currentContext());
        return;
      }

      throwable = t;
      done = true;
      CoreSubscriber<? super T> i = inner;

      if (i != null) {
        if (first == null) {
          drainRegular();
        }
      } else {
        Operators.error(outer, t);
      }
    }

    @Override
    public void onComplete() {
      if (done) {
        return;
      }

      done = true;
      CoreSubscriber<? super T> i = inner;

      if (i != null) {
        if (first == null) {
          drainRegular();
        }
      } else {
        Operators.complete(outer);
      }
    }

    @Override
    public void request(long n) {
      if (Operators.validate(n)) {
        if (first != null && drainRegular() && n != Long.MAX_VALUE) {
          if (--n > 0) {
            s.request(n);
          }
        } else {
          s.request(n);
        }
      }
    }

    boolean drainRegular() {
      if (WIP.getAndIncrement(this) != 0) {
        return false;
      }

      T f = first;
      int m = 1;
      boolean sent = false;
      Subscription s = this.s;
      CoreSubscriber<? super T> a = inner;

      for (; ; ) {
        if (f != null) {
          first = null;

          if (s == Operators.cancelledSubscription()) {
            Operators.onDiscard(f, a.currentContext());
            return true;
          }

          a.onNext(f);
          f = null;
          sent = true;
        }

        if (s == Operators.cancelledSubscription()) {
          return sent;
        }

        if (done) {
          Throwable t = throwable;
          if (t != null) {
            a.onError(t);
          } else {
            a.onComplete();
          }
          return sent;
        }

        m = WIP.addAndGet(this, -m);

        if (m == 0) {
          return sent;
        }
      }
    }
  }

  static final class SwitchTransformConditionalOperator<T, R> extends Flux<T>
      implements Fuseable.ConditionalSubscriber<T>, Subscription, Scannable {

    final Fuseable.ConditionalSubscriber<? super R> outer;
    final BiFunction<T, Flux<T>, Publisher<? extends R>> transformer;

    Subscription s;
    Throwable throwable;

    volatile boolean done;
    volatile T first;

    volatile Fuseable.ConditionalSubscriber<? super T> inner;

    @SuppressWarnings("rawtypes")
    static final AtomicReferenceFieldUpdater<
            SwitchTransformConditionalOperator, Fuseable.ConditionalSubscriber>
        INNER =
            AtomicReferenceFieldUpdater.newUpdater(
                SwitchTransformConditionalOperator.class,
                Fuseable.ConditionalSubscriber.class,
                "inner");

    volatile int wip;

    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<SwitchTransformConditionalOperator> WIP =
        AtomicIntegerFieldUpdater.newUpdater(SwitchTransformConditionalOperator.class, "wip");

    volatile int once;

    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<SwitchTransformConditionalOperator> ONCE =
        AtomicIntegerFieldUpdater.newUpdater(SwitchTransformConditionalOperator.class, "once");

    SwitchTransformConditionalOperator(
        Fuseable.ConditionalSubscriber<? super R> outer,
        BiFunction<T, Flux<T>, Publisher<? extends R>> transformer) {
      this.outer = outer;
      this.transformer = transformer;
    }

    @Override
    @Nullable
    public Object scanUnsafe(Attr key) {
      if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();
      if (key == Attr.PREFETCH) return 1;

      return null;
    }

    @Override
    public Context currentContext() {
      CoreSubscriber<? super T> actual = inner;

      if (actual != null) {
        return actual.currentContext();
      }

      return outer.currentContext();
    }

    @Override
    public void cancel() {
      if (s != Operators.cancelledSubscription()) {
        Subscription s = this.s;
        this.s = Operators.cancelledSubscription();

        if (WIP.getAndIncrement(this) == 0) {
          INNER.lazySet(this, null);

          T f = first;
          if (f != null) {
            first = null;
            Operators.onDiscard(f, currentContext());
          }
        }

        s.cancel();
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void subscribe(CoreSubscriber<? super T> actual) {
      if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
        if (actual instanceof Fuseable.ConditionalSubscriber) {
          INNER.lazySet(this, (Fuseable.ConditionalSubscriber<? super T>) actual);
        } else {
          INNER.lazySet(this, new ConditionalSubscriberAdapter<>(actual));
        }
        actual.onSubscribe(this);
      } else {
        Operators.error(
            actual, new IllegalStateException("SwitchTransform allows only one Subscriber"));
      }
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
      if (done) {
        Operators.onNextDropped(t, currentContext());
        return;
      }

      CoreSubscriber<? super T> i = inner;

      if (i == null) {
        try {
          first = t;
          Publisher<? extends R> result =
              Objects.requireNonNull(
                  transformer.apply(t, this), "The transformer returned a null value");
          result.subscribe(outer);
          return;
        } catch (Throwable e) {
          onError(Operators.onOperatorError(s, e, t, currentContext()));
          return;
        }
      }

      i.onNext(t);
    }

    @Override
    public boolean tryOnNext(T t) {
      if (done) {
        Operators.onNextDropped(t, currentContext());
        return false;
      }

      Fuseable.ConditionalSubscriber<? super T> i = inner;

      if (i == null) {
        try {
          first = t;
          Publisher<? extends R> result =
              Objects.requireNonNull(
                  transformer.apply(t, this), "The transformer returned a null value");
          result.subscribe(outer);
          return true;
        } catch (Throwable e) {
          onError(Operators.onOperatorError(s, e, t, currentContext()));
          return false;
        }
      }

      return i.tryOnNext(t);
    }

    @Override
    public void onError(Throwable t) {
      if (done) {
        Operators.onErrorDropped(t, currentContext());
        return;
      }

      throwable = t;
      done = true;
      CoreSubscriber<? super T> i = inner;

      if (i != null) {
        if (first == null) {
          drainRegular();
        }
      } else {
        Operators.error(outer, t);
      }
    }

    @Override
    public void onComplete() {
      if (done) {
        return;
      }

      done = true;
      CoreSubscriber<? super T> i = inner;

      if (i != null) {
        if (first == null) {
          drainRegular();
        }
      } else {
        Operators.complete(outer);
      }
    }

    @Override
    public void request(long n) {
      if (Operators.validate(n)) {
        if (first != null && drainRegular() && n != Long.MAX_VALUE) {
          if (--n > 0) {
            s.request(n);
          }
        } else {
          s.request(n);
        }
      }
    }

    boolean drainRegular() {
      if (WIP.getAndIncrement(this) != 0) {
        return false;
      }

      T f = first;
      int m = 1;
      boolean sent = false;
      Subscription s = this.s;
      CoreSubscriber<? super T> a = inner;

      for (; ; ) {
        if (f != null) {
          first = null;

          if (s == Operators.cancelledSubscription()) {
            Operators.onDiscard(f, a.currentContext());
            return true;
          }

          a.onNext(f);
          f = null;
          sent = true;
        }

        if (s == Operators.cancelledSubscription()) {
          return sent;
        }

        if (done) {
          Throwable t = throwable;
          if (t != null) {
            a.onError(t);
          } else {
            a.onComplete();
          }
          return sent;
        }

        m = WIP.addAndGet(this, -m);

        if (m == 0) {
          return sent;
        }
      }
    }
  }

  static final class ConditionalSubscriberAdapter<T> implements Fuseable.ConditionalSubscriber<T> {

    final CoreSubscriber<T> delegate;

    ConditionalSubscriberAdapter(CoreSubscriber<T> delegate) {
      this.delegate = delegate;
    }

    @Override
    public Context currentContext() {
      return delegate.currentContext();
    }

    @Override
    public void onSubscribe(Subscription s) {
      delegate.onSubscribe(s);
    }

    @Override
    public void onNext(T t) {
      delegate.onNext(t);
    }

    @Override
    public void onError(Throwable t) {
      delegate.onError(t);
    }

    @Override
    public void onComplete() {
      delegate.onComplete();
    }

    @Override
    public boolean tryOnNext(T t) {
      delegate.onNext(t);
      return true;
    }
  }
}
