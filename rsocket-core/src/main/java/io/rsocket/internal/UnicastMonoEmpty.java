package io.rsocket.internal;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;

/**
 * Represents an empty publisher which only calls onSubscribe and onComplete and allows only a
 * single subscriber.
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
public final class UnicastMonoEmpty extends Mono<Object> implements Scannable {

  final Runnable onSubscribe;

  volatile int once;

  @SuppressWarnings("rawtypes")
  static final AtomicIntegerFieldUpdater<UnicastMonoEmpty> ONCE =
      AtomicIntegerFieldUpdater.newUpdater(UnicastMonoEmpty.class, "once");

  UnicastMonoEmpty(Runnable onSubscribe) {
    this.onSubscribe = onSubscribe;
  }

  @Override
  public void subscribe(CoreSubscriber<? super Object> actual) {
    if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
      onSubscribe.run();
      Operators.complete(actual);
    } else {
      Operators.error(
          actual, new IllegalStateException("UnicastMonoEmpty allows only a single Subscriber"));
    }
  }

  /**
   * Returns a properly parametrized instance of this empty Publisher.
   *
   * @param <T> the output type
   * @return a properly parametrized instance of this empty Publisher
   */
  @SuppressWarnings("unchecked")
  public static <T> Mono<T> newInstance(Runnable onSubscribe) {
    return (Mono<T>) new UnicastMonoEmpty(onSubscribe);
  }

  @Override
  @Nullable
  public Object block(Duration m) {
    if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
      onSubscribe.run();
      return null;
    } else {
      throw new IllegalStateException("UnicastMonoEmpty allows only a single Subscriber");
    }
  }

  @Override
  @Nullable
  public Object block() {
    if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
      onSubscribe.run();
      return null;
    } else {
      throw new IllegalStateException("UnicastMonoEmpty allows only a single Subscriber");
    }
  }

  @Override
  public Object scanUnsafe(Attr key) {
    return null; // no particular key to be represented, still useful in hooks
  }

  @Override
  public String stepName() {
    return "source(UnicastMonoEmpty)";
  }
}
