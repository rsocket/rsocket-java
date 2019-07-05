package io.rsocket.internal;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Operators;
import reactor.core.publisher.SignalType;
import reactor.util.context.Context;

public class RequestResponseProcessor extends BaseSubscriber implements Processor {
  @SuppressWarnings("rawtypes")
  private static final AtomicIntegerFieldUpdater<RequestResponseProcessor> ONCE =
      AtomicIntegerFieldUpdater.newUpdater(RequestResponseProcessor.class, "once");

  private final Consumer<Subscription> onSubscribe;

  private final Consumer<Throwable> onError;

  private final Consumer<SignalType> onFinally;

  public RequestResponseProcessor(
      Consumer<Subscription> onSubscribe,
      Consumer<Throwable> onError,
      Consumer<SignalType> onFinally) {
    this.onSubscribe = onSubscribe;
    this.onError = onError;
    this.onFinally = onFinally;
  }

  @SuppressWarnings("unused")
  private volatile int once;

  private Subscriber actual;

  @Override
  public void subscribe(Subscriber actual) {
    Objects.requireNonNull(actual, "subscribe");
    if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
      this.actual = actual;
    } else {
      Operators.error(
          actual,
          new IllegalStateException("UnicastMonoProcessor allows only a single Subscriber"));
    }
  }

  /*
  @Override
  public void subscribe(CoreSubscriber<? super O> actual) {
    Objects.requireNonNull(actual, "subscribe");
    if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
      processor.subscribe(actual);
    } else {
      Operators.error(
          actual,
          new IllegalStateException("UnicastMonoProcessor allows only a single Subscriber"));
    }
  }
   */

  @Override
  public Context currentContext() {
    return null;
  }

  @Override
  protected void hookOnSubscribe(Subscription subscription) {
    onSubscribe.accept(subscription);
  }

  @Override
  protected void hookOnNext(Object value) {
    actual.onNext(value);
  }

  @Override
  protected void hookOnError(Throwable throwable) {
    actual.onError(throwable);
    onError.accept(throwable);
  }

  @Override
  protected void hookFinally(SignalType type) {
    actual.onComplete();
    onFinally.accept(type);
  }
}
