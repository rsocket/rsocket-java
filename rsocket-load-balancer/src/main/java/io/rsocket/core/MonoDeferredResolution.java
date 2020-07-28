package io.rsocket.core;

import io.netty.util.ReferenceCountUtil;
import io.rsocket.Payload;
import io.rsocket.frame.FrameType;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BiConsumer;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

abstract class MonoDeferredResolution<RESULT, R> extends Mono<RESULT>
    implements CoreSubscriber<RESULT>, Subscription, Scannable, BiConsumer<R, Throwable> {

  final ResolvingOperator<R> parent;
  final Payload payload;
  final FrameType requestType;

  volatile long requested;

  @SuppressWarnings("rawtypes")
  static final AtomicLongFieldUpdater<MonoDeferredResolution> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(MonoDeferredResolution.class, "requested");

  static final long STATE_UNSUBSCRIBED = -1;
  static final long STATE_SUBSCRIBER_SET = 0;
  static final long STATE_SUBSCRIBED = -2;
  static final long STATE_TERMINATED = Long.MIN_VALUE;

  Subscription s;
  CoreSubscriber<? super RESULT> actual;
  boolean done;

  MonoDeferredResolution(ResolvingOperator<R> parent, Payload payload, FrameType requestType) {
    this.parent = parent;
    this.payload = payload;
    this.requestType = requestType;

    REQUESTED.lazySet(this, STATE_UNSUBSCRIBED);
  }

  @Override
  public void subscribe(CoreSubscriber<? super RESULT> actual) {
    if (this.requested == STATE_UNSUBSCRIBED
        && REQUESTED.compareAndSet(this, STATE_UNSUBSCRIBED, STATE_SUBSCRIBER_SET)) {

      actual.onSubscribe(this);

      if (this.requested == STATE_TERMINATED) {
        return;
      }

      this.actual = actual;
      this.parent.observe(this);
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
      return state == STATE_TERMINATED;
    }

    return null;
  }

  @Override
  public final void onSubscribe(Subscription s) {
    final long state = this.requested;
    Subscription a = this.s;
    if (state == STATE_TERMINATED) {
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

      if (r == STATE_TERMINATED || r == STATE_SUBSCRIBED) {
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
  public final void onNext(RESULT payload) {
    this.actual.onNext(payload);
  }

  @Override
  public void onError(Throwable t) {
    if (this.done) {
      Operators.onErrorDropped(t, this.actual.currentContext());
      return;
    }

    this.done = true;
    this.actual.onError(t);
  }

  @Override
  public void onComplete() {
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

      if (r == STATE_TERMINATED) { // if canceled, just exit
        return;
      }

      // if onSubscribe -> subscription exists (and we sure of that because volatile read
      // after volatile write) so we can execute requestN on the subscription
      this.s.request(n);
    }
  }

  public void cancel() {
    long state = REQUESTED.getAndSet(this, STATE_TERMINATED);
    if (state == STATE_TERMINATED) {
      return;
    }

    if (state == STATE_SUBSCRIBED) {
      this.s.cancel();
    } else {
      this.parent.remove(this);
      ReferenceCountUtil.safeRelease(this.payload);
    }
  }

  boolean isTerminated() {
    return this.requested == STATE_TERMINATED;
  }
}
