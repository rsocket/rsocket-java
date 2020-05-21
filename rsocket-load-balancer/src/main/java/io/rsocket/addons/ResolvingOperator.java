package io.rsocket.addons;

import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

public class ResolvingOperator<T> implements Disposable {

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

  final void subscribe(BiConsumer<T, Throwable> actual) {
    final int state = add(actual);

    if (state == READY_STATE) {
      actual.accept(value, null);
    } else if (state == TERMINATED_STATE) {
      actual.accept(null, t);
    }
  }

  @SuppressWarnings("unchecked")
  final void terminate(Throwable t) {
    if (isDisposed()) {
      return;
    }

    // writes happens before volatile write
    this.t = t;

    final BiConsumer<T, Throwable>[] subscribers = SUBSCRIBERS.getAndSet(this, TERMINATED);
    if (subscribers == TERMINATED) {
      Operators.onErrorDropped(t, Context.empty());
      return;
    }

    this.doFinally();

    for (BiConsumer<T, Throwable> consumer : subscribers) {
      consumer.accept(null, t);
    }
  }

  final void complete() {
    BiConsumer<T, Throwable>[] subscribers = this.subscribers;
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
        this.doOnDispose();
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
          this.doOnDispose();
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

  abstract static class MonoDeferredResolution<T, R> extends Mono<T>
      implements CoreSubscriber<T>, Subscription, Scannable, BiConsumer<R, Throwable> {

    final ResolvingOperator<R> parent;

    volatile long requested;

    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<MonoDeferredResolution> REQUESTED =
        AtomicLongFieldUpdater.newUpdater(MonoDeferredResolution.class, "requested");

    static final long STATE_UNSUBSCRIBED = -1;
    static final long STATE_SUBSCRIBER_SET = 0;
    static final long STATE_SUBSCRIBED = -2;
    static final long STATE_CANCELLED = Long.MIN_VALUE;

    Subscription s;
    CoreSubscriber<? super T> actual;
    boolean done;

    MonoDeferredResolution(ResolvingOperator<R> parent) {
      this.parent = parent;

      REQUESTED.lazySet(this, STATE_UNSUBSCRIBED);
    }

    @Override
    public final void subscribe(CoreSubscriber<? super T> actual) {
      if (this.requested == STATE_UNSUBSCRIBED
          && REQUESTED.compareAndSet(this, STATE_UNSUBSCRIBED, STATE_SUBSCRIBER_SET)) {

        actual.onSubscribe(this);

        if (this.requested == STATE_CANCELLED) {
          return;
        }

        this.actual = actual;
        this.parent.subscribe(this);
      } else {
        Operators.error(actual, new IllegalStateException("Only a single Subscriber allowed"));
      }
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
    public final void request(long n) {
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

  abstract static class FluxDeferredResolution<T, R> extends Flux<T>
      implements CoreSubscriber<T>, Subscription, BiConsumer<R, Throwable>, Scannable {

    final ResolvingOperator<R> parent;

    volatile long requested;

    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<FluxDeferredResolution> REQUESTED =
        AtomicLongFieldUpdater.newUpdater(FluxDeferredResolution.class, "requested");

    static final long STATE_UNSUBSCRIBED = -1;
    static final long STATE_SUBSCRIBER_SET = 0;
    static final long STATE_SUBSCRIBED = -2;
    static final long STATE_CANCELLED = Long.MIN_VALUE;

    Subscription s;
    CoreSubscriber<? super T> actual;
    boolean done;

    FluxDeferredResolution(ResolvingOperator<R> parent) {
      this.parent = parent;

      REQUESTED.lazySet(this, STATE_UNSUBSCRIBED);
    }

    @Override
    public final void subscribe(CoreSubscriber<? super T> actual) {
      if (this.requested == STATE_UNSUBSCRIBED
          && REQUESTED.compareAndSet(this, STATE_UNSUBSCRIBED, STATE_SUBSCRIBER_SET)) {

        actual.onSubscribe(this);

        if (this.requested == STATE_CANCELLED) {
          return;
        }

        this.actual = actual;
        this.parent.subscribe(this);
      } else {
        Operators.error(actual, new IllegalStateException("Only a single Subscriber allowed"));
      }
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
    public final void request(long n) {
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
}
