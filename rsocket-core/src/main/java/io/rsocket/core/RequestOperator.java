package io.rsocket.core;

import io.rsocket.Payload;
import java.util.concurrent.atomic.AtomicInteger;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.publisher.Operators;
import reactor.core.publisher.SignalType;
import reactor.util.context.Context;

/**
 * This class is a supportive class to simplify requests' logic implementation and aimed to be used
 * with {@link Operators#lift}
 *
 * <p>This class incorporates extra logic which ensures serial cancel and first request execution as
 * well as provides extra logic to separate first {@link Subscription#request} invocation from the
 * rest of them
 */
abstract class RequestOperator
    implements CoreSubscriber<Payload>, Fuseable.QueueSubscription<Payload> {

  final CoreSubscriber<? super Payload> actual;

  Subscription s;
  Fuseable.QueueSubscription<Payload> qs;

  boolean firstRequest = true;

  final AtomicInteger wip = new AtomicInteger(0);

  RequestOperator(CoreSubscriber<? super Payload> actual) {
    this.actual = actual;
  }

  /**
   * Optional hook executed exactly once on the first {@link Subscription#request) invocation
   * and right after the {@link Subscription#request} was propagated to the upstream subscription.
   *
   * <p><b>Note</b>: this hook may not be invoked if cancellation happened before this invocation
   */
  void hookOnFirstRequest(long n) {}

  /**
   * Optional hook executed after the {@link Subscription#request} was propagated to the upstream
   * subscription and excludes the first {@link Subscription#request} invocation.
   */
  void hookOnRestRequests(long n) {}

  /** Optional hook executed after this {@link Subscription} cancelling. */
  void hookOnCancel() {}

  /**
   * Optional hook executed after {@link org.reactivestreams.Subscriber} termination events
   * (onError, onComplete).
   *
   * @param signalType the type of termination event that triggered the hook ({@link
   *     SignalType#ON_ERROR} or {@link SignalType#ON_COMPLETE})
   */
  void hookOnTerminal(SignalType signalType) {}

  @Override
  public Context currentContext() {
    return actual.currentContext();
  }

  @Override
  public void request(long n) {
    this.s.request(n);
    if (!firstRequest) {
      hookOnRestRequests(n);
      return;
    }
    this.firstRequest = false;

    if (wip.getAndIncrement() != 0) {
      return;
    }
    int missed = 1;

    boolean firstLoop = true;
    for (; ; ) {
      if (firstLoop) {
        firstLoop = false;
        hookOnFirstRequest(n);
      } else {
        hookOnCancel();
        return;
      }

      missed = wip.addAndGet(-missed);
      if (missed == 0) {
        return;
      }
    }
  }

  @Override
  public void cancel() {
    this.s.cancel();

    if (wip.getAndIncrement() != 0) {
      return;
    }

    hookOnCancel();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void onSubscribe(Subscription s) {
    if (Operators.validate(this.s, s)) {
      this.s = s;
      if (s instanceof Fuseable.QueueSubscription) {
        this.qs = (Fuseable.QueueSubscription<Payload>) s;
      }
      this.actual.onSubscribe(this);
    }
  }

  @Override
  public void onNext(Payload t) {
    this.actual.onNext(t);
  }

  @Override
  public void onError(Throwable t) {
    this.actual.onError(t);
    this.hookOnTerminal(SignalType.ON_ERROR);
  }

  @Override
  public void onComplete() {
    this.actual.onComplete();
    this.hookOnTerminal(SignalType.ON_COMPLETE);
  }

  @Override
  public int requestFusion(int requestedMode) {
    if (this.qs != null) {
      return this.qs.requestFusion(requestedMode);
    } else {
      return Fuseable.NONE;
    }
  }

  @Override
  public Payload poll() {
    return this.qs.poll();
  }

  @Override
  public int size() {
    return this.qs.size();
  }

  @Override
  public boolean isEmpty() {
    return this.qs.isEmpty();
  }

  @Override
  public void clear() {
    this.qs.clear();
  }
}
