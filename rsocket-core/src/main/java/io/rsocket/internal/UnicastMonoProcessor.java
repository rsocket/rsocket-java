package io.rsocket.internal;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;
import reactor.util.function.Tuple2;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

public class UnicastMonoProcessor<O> extends Mono<O>
    implements Processor<O, O>,
        CoreSubscriber<O>,
        Disposable,
        Subscription,
        Scannable,
        LongSupplier {

  @SuppressWarnings("rawtypes")
  static final AtomicIntegerFieldUpdater<UnicastMonoProcessor> ONCE =
      AtomicIntegerFieldUpdater.newUpdater(UnicastMonoProcessor.class, "once");

  private final MonoProcessor<O> processor;

  @SuppressWarnings("unused")
  private volatile int once;

  private UnicastMonoProcessor() {
    this.processor = MonoProcessor.create();
  }

  public static <O> UnicastMonoProcessor<O> create() {
    return new UnicastMonoProcessor<>();
  }

  @Override
  public Stream<? extends Scannable> actuals() {
    return processor.actuals();
  }

  @Override
  public boolean isScanAvailable() {
    return processor.isScanAvailable();
  }

  @Override
  public String name() {
    return processor.name();
  }

  @Override
  public String stepName() {
    return processor.stepName();
  }

  @Override
  public Stream<String> steps() {
    return processor.steps();
  }

  @Override
  public Stream<? extends Scannable> parents() {
    return processor.parents();
  }

  @Override
  @Nullable
  public <T> T scan(Attr<T> key) {
    return processor.scan(key);
  }

  @Override
  public <T> T scanOrDefault(Attr<T> key, T defaultValue) {
    return processor.scanOrDefault(key, defaultValue);
  }

  @Override
  public Stream<Tuple2<String, String>> tags() {
    return processor.tags();
  }

  @Override
  public long getAsLong() {
    return processor.getAsLong();
  }

  @Override
  public void onSubscribe(Subscription s) {
    processor.onSubscribe(s);
  }

  @Override
  public void onNext(O o) {
    processor.onNext(o);
  }

  @Override
  public void onError(Throwable t) {
    processor.onError(t);
  }

  @Nullable
  public Throwable getError() {
    return processor.getError();
  }

  public boolean isCancelled() {
    return processor.isCancelled();
  }

  public boolean isError() {
    return processor.isError();
  }

  public boolean isSuccess() {
    return processor.isSuccess();
  }

  public boolean isTerminated() {
    return processor.isTerminated();
  }

  @Nullable
  public O peek() {
    return processor.peek();
  }

  public long downstreamCount() {
    return processor.downstreamCount();
  }

  public boolean hasDownstreams() {
    return processor.hasDownstreams();
  }

  @Override
  public void onComplete() {
    processor.onComplete();
  }

  @Override
  public void request(long n) {
    processor.request(n);
  }

  @Override
  public void cancel() {
    processor.cancel();
  }

  @Override
  public void dispose() {
    processor.dispose();
  }

  @Override
  public Context currentContext() {
    return processor.currentContext();
  }

  @Override
  public boolean isDisposed() {
    return processor.isDisposed();
  }

  @Override
  public Object scanUnsafe(Attr key) {
    return processor.scanUnsafe(key);
  }

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
}
