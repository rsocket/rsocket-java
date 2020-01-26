/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
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
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Signal;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * @author Oleh Dokuka
 * @param <T>
 * @param <R>
 */
public final class FluxSwitchOnFirst<T, R> extends FluxOperator<T, R> {
  static final int STATE_CANCELLED = -2;
  static final int STATE_REQUESTED = -1;

  static final int STATE_INIT = 0;
  static final int STATE_SUBSCRIBED_ONCE = 1;

  final BiFunction<Signal<? extends T>, Flux<T>, Publisher<? extends R>> transformer;
  final boolean cancelSourceOnComplete;

  volatile int once;

  @SuppressWarnings("rawtypes")
  static final AtomicIntegerFieldUpdater<FluxSwitchOnFirst> ONCE =
      AtomicIntegerFieldUpdater.newUpdater(FluxSwitchOnFirst.class, "once");

  public FluxSwitchOnFirst(
      Flux<? extends T> source,
      BiFunction<Signal<? extends T>, Flux<T>, Publisher<? extends R>> transformer,
      boolean cancelSourceOnComplete) {
    super(source);
    this.transformer = Objects.requireNonNull(transformer, "transformer");
    this.cancelSourceOnComplete = cancelSourceOnComplete;
  }

  @Override
  public int getPrefetch() {
    return 1;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void subscribe(CoreSubscriber<? super R> actual) {
    Objects.requireNonNull(actual, "subscribe");
    if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
      if (actual instanceof Fuseable.ConditionalSubscriber) {
        source.subscribe(
            new SwitchOnFirstConditionalMain<>(
                (Fuseable.ConditionalSubscriber<? super R>) actual,
                transformer,
                cancelSourceOnComplete));
        return;
      }
      source.subscribe(new SwitchOnFirstMain<>(actual, transformer, cancelSourceOnComplete));
    } else {
      Operators.error(actual, new IllegalStateException("Allows only a single Subscriber"));
    }
  }

  abstract static class AbstractSwitchOnFirstMain<T, R> extends Flux<T>
      implements CoreSubscriber<T>, Scannable, Subscription {

    final ControlSubscriber<? super R> outer;
    final BiFunction<Signal<? extends T>, Flux<T>, Publisher<? extends R>> transformer;

    Subscription s;
    Throwable throwable;
    boolean first = true;
    boolean done;

    volatile CoreSubscriber<? super T> inner;

    @SuppressWarnings("rawtypes")
    static final AtomicReferenceFieldUpdater<AbstractSwitchOnFirstMain, CoreSubscriber> INNER =
        AtomicReferenceFieldUpdater.newUpdater(
            AbstractSwitchOnFirstMain.class, CoreSubscriber.class, "inner");

    volatile int state;

    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<AbstractSwitchOnFirstMain> STATE =
        AtomicIntegerFieldUpdater.newUpdater(AbstractSwitchOnFirstMain.class, "state");

    @SuppressWarnings("unchecked")
    AbstractSwitchOnFirstMain(
        CoreSubscriber<? super R> outer,
        BiFunction<Signal<? extends T>, Flux<T>, Publisher<? extends R>> transformer,
        boolean cancelSourceOnComplete) {
      this.outer =
          outer instanceof Fuseable.ConditionalSubscriber
              ? new SwitchOnFirstConditionalControlSubscriber<>(
                  this, (Fuseable.ConditionalSubscriber<R>) outer, cancelSourceOnComplete)
              : new SwitchOnFirstControlSubscriber<>(this, outer, cancelSourceOnComplete);
      this.transformer = transformer;
    }

    @Override
    @Nullable
    public Object scanUnsafe(Attr key) {
      CoreSubscriber<? super T> i = this.inner;

      if (key == Attr.CANCELLED) return i == CancelledSubscriber.INSTANCE;
      if (key == Attr.TERMINATED) return done || i == CancelledSubscriber.INSTANCE;

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
      if (INNER.getAndSet(this, CancelledSubscriber.INSTANCE) != CancelledSubscriber.INSTANCE) {
        s.cancel();
      }
    }

    @Override
    public void onSubscribe(Subscription s) {
      if (Operators.validate(this.s, s)) {
        this.s = s;
        outer.sendSubscription();
        s.request(1);
      }
    }

    @Override
    public void onNext(T t) {
      CoreSubscriber<? super T> i = inner;
      if (i == CancelledSubscriber.INSTANCE || done) {
        Operators.onNextDropped(t, currentContext());
        return;
      }

      if (i == null) {
        Publisher<? extends R> result;
        CoreSubscriber<? super R> o = outer;

        try {
          result =
              Objects.requireNonNull(
                  transformer.apply(Signal.next(t, o.currentContext()), this),
                  "The transformer returned a null value");
        } catch (Throwable e) {
          done = true;
          Operators.error(o, Operators.onOperatorError(s, e, t, o.currentContext()));
          return;
        }

        first = false;
        result.subscribe(o);
        return;
      }

      i.onNext(t);
    }

    @Override
    public void onError(Throwable t) {
      CoreSubscriber<? super T> i = inner;
      if (i == CancelledSubscriber.INSTANCE || done) {
        Operators.onErrorDropped(t, currentContext());
        return;
      }

      throwable = t;
      done = true;

      if (first && i == null) {
        Publisher<? extends R> result;
        CoreSubscriber<? super R> o = outer;

        try {
          result =
              Objects.requireNonNull(
                  transformer.apply(Signal.error(t, o.currentContext()), this),
                  "The transformer returned a null value");
        } catch (Throwable e) {
          done = true;
          Operators.error(o, Operators.onOperatorError(s, e, t, o.currentContext()));
          return;
        }

        first = false;
        result.subscribe(o);
      }

      i = this.inner;
      if (i != null) {
        i.onError(t);
      }
    }

    @Override
    public void onComplete() {
      CoreSubscriber<? super T> i = inner;
      if (i == CancelledSubscriber.INSTANCE || done) {
        return;
      }

      done = true;

      if (first && i == null) {
        Publisher<? extends R> result;
        CoreSubscriber<? super R> o = outer;

        try {
          result =
              Objects.requireNonNull(
                  transformer.apply(Signal.complete(o.currentContext()), this),
                  "The transformer returned a null value");
        } catch (Throwable e) {
          done = true;
          Operators.error(o, Operators.onOperatorError(s, e, null, o.currentContext()));
          return;
        }

        first = false;
        result.subscribe(o);
      }

      i = inner;
      if (i != null) {
        i.onComplete();
      }
    }

    @Override
    public void request(long n) {
      if (Operators.validate(n)) {
        s.request(n);
      }
    }
  }

  static final class SwitchOnFirstMain<T, R> extends AbstractSwitchOnFirstMain<T, R> {

    SwitchOnFirstMain(
        CoreSubscriber<? super R> outer,
        BiFunction<Signal<? extends T>, Flux<T>, Publisher<? extends R>> transformer,
        boolean cancelSourceOnComplete) {
      super(outer, transformer, cancelSourceOnComplete);
    }

    @Override
    public void subscribe(CoreSubscriber<? super T> actual) {
      if (state == STATE_INIT && STATE.compareAndSet(this, STATE_INIT, STATE_SUBSCRIBED_ONCE)) {
        if (done) {
          if (throwable != null) {
            Operators.error(actual, throwable);
          } else {
            Operators.complete(actual);
          }
          return;
        }
        INNER.lazySet(this, actual);
        actual.onSubscribe(this);
      } else {
        Operators.error(
            actual, new IllegalStateException("FluxSwitchOnFirst allows only one Subscriber"));
      }
    }
  }

  static final class SwitchOnFirstConditionalMain<T, R> extends AbstractSwitchOnFirstMain<T, R>
      implements Fuseable.ConditionalSubscriber<T> {

    SwitchOnFirstConditionalMain(
        Fuseable.ConditionalSubscriber<? super R> outer,
        BiFunction<Signal<? extends T>, Flux<T>, Publisher<? extends R>> transformer,
        boolean cancelSourceOnComplete) {
      super(outer, transformer, cancelSourceOnComplete);
    }

    @Override
    public void subscribe(CoreSubscriber<? super T> actual) {
      if (state == STATE_INIT && STATE.compareAndSet(this, STATE_INIT, STATE_SUBSCRIBED_ONCE)) {
        if (done) {
          if (throwable != null) {
            Operators.error(actual, throwable);
          } else {
            Operators.complete(actual);
          }
          return;
        }
        INNER.lazySet(this, Operators.toConditionalSubscriber(actual));
        actual.onSubscribe(this);
      } else {
        Operators.error(
            actual, new IllegalStateException("FluxSwitchOnFirst allows only one Subscriber"));
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean tryOnNext(T t) {
      CoreSubscriber<? super T> i = inner;
      if (i == CancelledSubscriber.INSTANCE || done) {
        Operators.onNextDropped(t, currentContext());
        return false;
      }

      if (i == null) {
        Publisher<? extends R> result;
        CoreSubscriber<? super R> o = outer;

        try {
          result =
              Objects.requireNonNull(
                  transformer.apply(Signal.next(t, o.currentContext()), this),
                  "The transformer returned a null value");
        } catch (Throwable e) {
          done = true;
          Operators.error(o, Operators.onOperatorError(s, e, t, o.currentContext()));
          return false;
        }

        first = false;
        result.subscribe(o);
        return true;
      }

      return ((Fuseable.ConditionalSubscriber<? super T>) i).tryOnNext(t);
    }
  }

  static final class SwitchOnFirstControlSubscriber<T>
      implements ControlSubscriber<T>, Scannable, Subscription {

    final AbstractSwitchOnFirstMain<?, T> parent;
    final CoreSubscriber<? super T> actual;
    final boolean cancelSourceOnComplete;

    volatile long requested;

    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<SwitchOnFirstControlSubscriber> REQUESTED =
        AtomicLongFieldUpdater.newUpdater(SwitchOnFirstControlSubscriber.class, "requested");

    Subscription s;

    SwitchOnFirstControlSubscriber(
        AbstractSwitchOnFirstMain<?, T> parent,
        CoreSubscriber<? super T> actual,
        boolean cancelSourceOnComplete) {
      this.parent = parent;
      this.actual = actual;
      this.cancelSourceOnComplete = cancelSourceOnComplete;
    }

    @Override
    public Context currentContext() {
      return actual.currentContext();
    }

    @Override
    public void sendSubscription() {
      actual.onSubscribe(this);
    }

    @Override
    public void onSubscribe(Subscription s) {
      if (this.s == null && this.requested != STATE_CANCELLED) {
        this.s = s;

        tryRequest();
      } else {
        s.cancel();
      }
    }

    @Override
    public void onNext(T t) {
      actual.onNext(t);
    }

    @Override
    public void onError(Throwable throwable) {
      if (!parent.done) {
        parent.cancel();
      }

      actual.onError(throwable);
    }

    @Override
    public void onComplete() {
      if (!parent.done && cancelSourceOnComplete) {
        parent.cancel();
      }

      actual.onComplete();
    }

    @Override
    public void request(long n) {
      long r = this.requested;

      if (r > STATE_REQUESTED) {
        long u;
        for (; ; ) {
          if (r == Long.MAX_VALUE) {
            return;
          }
          u = Operators.addCap(r, n);
          if (REQUESTED.compareAndSet(this, r, u)) {
            return;
          } else {
            r = requested;

            if (r < 0) {
              break;
            }
          }
        }
      }

      if (r == STATE_CANCELLED) {
        return;
      }

      s.request(n);
    }

    void tryRequest() {
      final Subscription s = this.s;
      long r = REQUESTED.getAndSet(this, -1);

      if (r > 0) {
        s.request(r);
      }
    }

    @Override
    public void cancel() {
      final long state = REQUESTED.getAndSet(this, STATE_CANCELLED);

      if (state == STATE_CANCELLED) {
        return;
      }

      if (state == STATE_REQUESTED) {
        s.cancel();
      }

      parent.cancel();
    }

    @Override
    public Object scanUnsafe(Attr key) {
      if (key == Attr.PARENT) return parent;
      if (key == Attr.ACTUAL) return actual;

      return null;
    }
  }

  static final class SwitchOnFirstConditionalControlSubscriber<T>
      implements ControlSubscriber<T>, Scannable, Subscription, Fuseable.ConditionalSubscriber<T> {

    final AbstractSwitchOnFirstMain<?, T> parent;
    final Fuseable.ConditionalSubscriber<? super T> actual;
    final boolean terminateUpstreamOnComplete;

    volatile long requested;

    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<SwitchOnFirstConditionalControlSubscriber> REQUESTED =
        AtomicLongFieldUpdater.newUpdater(
            SwitchOnFirstConditionalControlSubscriber.class, "requested");

    Subscription s;

    SwitchOnFirstConditionalControlSubscriber(
        AbstractSwitchOnFirstMain<?, T> parent,
        Fuseable.ConditionalSubscriber<? super T> actual,
        boolean terminateUpstreamOnComplete) {
      this.parent = parent;
      this.actual = actual;
      this.terminateUpstreamOnComplete = terminateUpstreamOnComplete;
    }

    @Override
    public void sendSubscription() {
      actual.onSubscribe(this);
    }

    @Override
    public Context currentContext() {
      return actual.currentContext();
    }

    @Override
    public void onSubscribe(Subscription s) {
      if (this.s == null && this.requested != STATE_CANCELLED) {
        this.s = s;

        tryRequest();
      } else {
        s.cancel();
      }
    }

    @Override
    public void onNext(T t) {
      actual.onNext(t);
    }

    @Override
    public boolean tryOnNext(T t) {
      return actual.tryOnNext(t);
    }

    @Override
    public void onError(Throwable throwable) {
      if (!parent.done) {
        parent.cancel();
      }

      actual.onError(throwable);
    }

    @Override
    public void onComplete() {
      if (!parent.done && terminateUpstreamOnComplete) {
        parent.cancel();
      }

      actual.onComplete();
    }

    @Override
    public void request(long n) {
      long r = this.requested;

      if (r > STATE_REQUESTED) {
        long u;
        for (; ; ) {
          if (r == Long.MAX_VALUE) {
            return;
          }
          u = Operators.addCap(r, n);
          if (REQUESTED.compareAndSet(this, r, u)) {
            return;
          } else {
            r = requested;

            if (r < 0) {
              break;
            }
          }
        }
      }

      if (r == STATE_CANCELLED) {
        return;
      }

      s.request(n);
    }

    void tryRequest() {
      final Subscription s = this.s;
      long r = REQUESTED.getAndSet(this, -1);

      if (r > 0) {
        s.request(r);
      }
    }

    @Override
    public void cancel() {
      final long state = REQUESTED.getAndSet(this, STATE_CANCELLED);

      if (state == STATE_CANCELLED) {
        return;
      }

      if (state == STATE_REQUESTED) {
        s.cancel();
        return;
      }

      parent.cancel();
    }

    @Override
    public Object scanUnsafe(Attr key) {
      if (key == Attr.PARENT) return parent;
      if (key == Attr.ACTUAL) return actual;

      return null;
    }
  }

  interface ControlSubscriber<T> extends CoreSubscriber<T> {

    void sendSubscription();
  }

  static final class CancelledSubscriber implements CoreSubscriber {

    static final CancelledSubscriber INSTANCE = new CancelledSubscriber();

    private CancelledSubscriber() {}

    @Override
    public void onSubscribe(Subscription s) {}

    @Override
    public void onNext(Object o) {}

    @Override
    public void onError(Throwable t) {}

    @Override
    public void onComplete() {}
  }
}
