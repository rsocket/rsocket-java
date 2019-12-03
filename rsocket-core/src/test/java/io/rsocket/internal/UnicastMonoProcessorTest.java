package io.rsocket.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.Operators;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.util.function.Tuple2;

public class UnicastMonoProcessorTest {

  @Test
  public void noRetentionOnTermination() throws InterruptedException {
    Date date = new Date();
    CompletableFuture<Date> future = new CompletableFuture<>();

    WeakReference<Date> refDate = new WeakReference<>(date);
    WeakReference<CompletableFuture<Date>> refFuture = new WeakReference<>(future);

    Mono<Date> source = Mono.fromFuture(future);
    Mono<String> data =
        source.map(Date::toString).log().subscribeWith(UnicastMonoProcessor.create()).log();

    future.complete(date);
    assertThat(data.block()).isEqualTo(date.toString());

    date = null;
    future = null;
    source = null;
    System.gc();

    int cycles;
    for (cycles = 10; cycles > 0; cycles--) {
      if (refDate.get() == null && refFuture.get() == null) break;
      Thread.sleep(100);
    }

    assumeThat(refFuture.get()).isNull();
    assertThat(refDate.get()).isNull();
    assertThat(cycles).isNotZero().isPositive();
  }

  @Test
  public void noRetentionOnTerminationError() throws InterruptedException {
    CompletableFuture<Date> future = new CompletableFuture<>();

    WeakReference<CompletableFuture<Date>> refFuture = new WeakReference<>(future);
    UnicastMonoProcessor<String> processor = UnicastMonoProcessor.create();

    Mono<Date> source = Mono.fromFuture(future);
    Mono<String> data = source.map(Date::toString).subscribeWith(processor);

    future.completeExceptionally(new IllegalStateException());

    assertThatExceptionOfType(IllegalStateException.class).isThrownBy(data::block);

    future = null;
    source = null;
    System.gc();

    int cycles;
    for (cycles = 10; cycles > 0; cycles--) {
      if (refFuture.get() == null) break;
      Thread.sleep(100);
    }

    assumeThat(refFuture.get()).isNull();
    assertThat(cycles).isNotZero().isPositive();
  }

  @Test
  public void noRetentionOnTerminationCancel() throws InterruptedException {
    CompletableFuture<Date> future = new CompletableFuture<>();

    WeakReference<CompletableFuture<Date>> refFuture = new WeakReference<>(future);
    UnicastMonoProcessor<String> processor = UnicastMonoProcessor.create();

    Mono<Date> source = Mono.fromFuture(future);
    Mono<String> data =
        source.map(Date::toString).transformDeferred((s) -> s.subscribeWith(processor));

    future = null;
    source = null;

    data.subscribe().dispose();
    processor.dispose();

    data = null;

    System.gc();

    int cycles;
    for (cycles = 10; cycles > 0; cycles--) {
      if (refFuture.get() == null) break;
      Thread.sleep(100);
    }

    assumeThat(refFuture.get()).isNull();
    assertThat(cycles).isNotZero().isPositive();
  }

  @Test(expected = IllegalStateException.class)
  public void MonoProcessorResultNotAvailable() {
    MonoProcessor<String> mp = MonoProcessor.create();
    mp.block(Duration.ofMillis(1));
  }

  @Test
  public void MonoProcessorRejectedDoOnSuccessOrError() {
    UnicastMonoProcessor<String> mp = UnicastMonoProcessor.create();
    AtomicReference<Throwable> ref = new AtomicReference<>();

    mp.doOnSuccessOrError((s, f) -> ref.set(f)).subscribe();
    mp.onError(new Exception("test"));

    assertThat(ref.get()).hasMessage("test");
    assertThat(mp.isError()).isTrue();
  }

  @Test
  public void MonoProcessorRejectedDoOnTerminate() {
    UnicastMonoProcessor<String> mp = UnicastMonoProcessor.create();
    AtomicInteger invoked = new AtomicInteger();

    mp.doOnTerminate(invoked::incrementAndGet).subscribe();
    mp.onError(new Exception("test"));

    assertThat(invoked.get()).isEqualTo(1);
    assertThat(mp.isError()).isTrue();
  }

  @Test
  public void MonoProcessorRejectedSubscribeCallback() {
    UnicastMonoProcessor<String> mp = UnicastMonoProcessor.create();
    AtomicReference<Throwable> ref = new AtomicReference<>();

    mp.subscribe(v -> {}, ref::set);
    mp.onError(new Exception("test"));

    assertThat(ref.get()).hasMessage("test");
    assertThat(mp.isError()).isTrue();
  }

  @Test
  public void MonoProcessorSuccessDoOnSuccessOrError() {
    UnicastMonoProcessor<String> mp = UnicastMonoProcessor.create();
    AtomicReference<String> ref = new AtomicReference<>();

    mp.doOnSuccessOrError((s, f) -> ref.set(s)).subscribe();
    mp.onNext("test");

    assertThat(ref.get()).isEqualToIgnoringCase("test");
    assertThat(mp.isDisposed()).isTrue();
    assertThat(mp.isError()).isFalse();
  }

  @Test
  public void MonoProcessorSuccessDoOnTerminate() {
    UnicastMonoProcessor<String> mp = UnicastMonoProcessor.create();
    AtomicInteger invoked = new AtomicInteger();

    mp.doOnTerminate(invoked::incrementAndGet).subscribe();
    mp.onNext("test");

    assertThat(invoked.get()).isEqualTo(1);
    assertThat(mp.isDisposed()).isTrue();
    assertThat(mp.isError()).isFalse();
  }

  @Test
  public void MonoProcessorSuccessSubscribeCallback() {
    UnicastMonoProcessor<String> mp = UnicastMonoProcessor.create();
    AtomicReference<String> ref = new AtomicReference<>();

    mp.subscribe(ref::set);
    mp.onNext("test");

    assertThat(ref.get()).isEqualToIgnoringCase("test");
    assertThat(mp.isDisposed()).isTrue();
    assertThat(mp.isError()).isFalse();
  }

  @Test
  public void MonoProcessorRejectedDoOnError() {
    UnicastMonoProcessor<String> mp = UnicastMonoProcessor.create();
    AtomicReference<Throwable> ref = new AtomicReference<>();

    mp.doOnError(ref::set).subscribe();
    mp.onError(new Exception("test"));

    assertThat(ref.get()).hasMessage("test");
    assertThat(mp.isError()).isTrue();
  }

  @Test(expected = NullPointerException.class)
  public void MonoProcessorRejectedSubscribeCallbackNull() {
    UnicastMonoProcessor<String> mp = UnicastMonoProcessor.create();

    mp.subscribe((Subscriber<String>) null);
  }

  @Test
  public void MonoProcessorSuccessDoOnSuccess() {
    UnicastMonoProcessor<String> mp = UnicastMonoProcessor.create();
    AtomicReference<String> ref = new AtomicReference<>();

    mp.doOnSuccess(ref::set).subscribe();
    mp.onNext("test");

    assertThat(ref.get()).isEqualToIgnoringCase("test");
    assertThat(mp.isDisposed()).isTrue();
    assertThat(mp.isError()).isFalse();
  }

  @Test
  public void MonoProcessorSuccessChainTogether() {
    UnicastMonoProcessor<String> mp = UnicastMonoProcessor.create();
    UnicastMonoProcessor<String> mp2 = UnicastMonoProcessor.create();
    mp.subscribe(mp2);

    mp.onNext("test");

    assertThat(mp2.peek()).isEqualToIgnoringCase("test");
    assertThat(mp.isDisposed()).isTrue();
    assertThat(mp.isError()).isFalse();
  }

  @Test
  public void MonoProcessorRejectedChainTogether() {
    UnicastMonoProcessor<String> mp = UnicastMonoProcessor.create();
    UnicastMonoProcessor<String> mp2 = UnicastMonoProcessor.create();
    mp.subscribe(mp2);

    mp.onError(new Exception("test"));

    assertThat(mp2.getError()).hasMessage("test");
    assertThat(mp.isError()).isTrue();
  }

  @Test
  public void MonoProcessorDoubleFulfill() {
    UnicastMonoProcessor<String> mp = UnicastMonoProcessor.create();

    StepVerifier.create(mp)
        .then(
            () -> {
              mp.onNext("test1");
              mp.onNext("test2");
            })
        .expectNext("test1")
        .expectComplete()
        .verifyThenAssertThat()
        .hasDroppedExactly("test2");
  }

  @Test
  public void MonoProcessorNullFulfill() {
    UnicastMonoProcessor<String> mp = UnicastMonoProcessor.create();

    mp.onNext(null);

    assertThat(mp.isDisposed()).isTrue();
    assertThat(mp.peek()).isNull();
  }

  @Test
  public void MonoProcessorMapFulfill() {
    UnicastMonoProcessor<Integer> mp = UnicastMonoProcessor.create();

    mp.onNext(1);

    UnicastMonoProcessor<Integer> mp2 =
        mp.map(s -> s * 2).subscribeWith(UnicastMonoProcessor.create());
    assertThat(mp2.peek()).isEqualTo(2);

    mp2.subscribe();
    assertThat(mp2.isDisposed()).isTrue();
    assertThat(mp2.peek()).isNull();
  }

  @Test
  public void MonoProcessorThenFulfill() {
    UnicastMonoProcessor<Integer> mp = UnicastMonoProcessor.create();

    mp.onNext(1);

    UnicastMonoProcessor<Integer> mp2 =
        mp.flatMap(s -> Mono.just(s * 2)).subscribeWith(UnicastMonoProcessor.create());
    mp2.subscribe();

    assertThat(mp2.isDisposed()).isTrue();
    assertThat(mp2.peek()).isEqualTo(2);
  }

  @Test
  public void MonoProcessorMapError() {
    UnicastMonoProcessor<Integer> mp = UnicastMonoProcessor.create();

    mp.onNext(1);

    UnicastMonoProcessor<Integer> mp2 = UnicastMonoProcessor.create();

    StepVerifier.create(
            mp.<Integer>map(
                    s -> {
                      throw new RuntimeException("test");
                    })
                .subscribeWith(mp2),
            0)
        .thenRequest(1)
        .then(
            () -> {
              assertThat(mp2.isDisposed()).isTrue();
              assertThat(mp2.getError()).hasMessage("test");
            })
        .verifyErrorMessage("test");
  }

  @Test(expected = Exception.class)
  public void MonoProcessorDoubleError() {
    UnicastMonoProcessor<String> mp = UnicastMonoProcessor.create();

    mp.onError(new Exception("test"));
    mp.onError(new Exception("test"));
  }

  @Test(expected = Exception.class)
  public void MonoProcessorDoubleSignal() {
    UnicastMonoProcessor<String> mp = UnicastMonoProcessor.create();

    mp.onNext("test");
    mp.onError(new Exception("test"));
  }

  @Test
  public void zipMonoProcessor() {
    UnicastMonoProcessor<Integer> mp = UnicastMonoProcessor.create();
    UnicastMonoProcessor<Integer> mp2 = UnicastMonoProcessor.create();
    UnicastMonoProcessor<Tuple2<Integer, Integer>> mp3 = UnicastMonoProcessor.create();

    StepVerifier.create(Mono.zip(mp, mp2).subscribeWith(mp3))
        .then(() -> assertThat(mp3.isDisposed()).isFalse())
        .then(() -> mp.onNext(1))
        .then(() -> assertThat(mp3.isDisposed()).isFalse())
        .then(() -> mp2.onNext(2))
        .then(
            () -> {
              assertThat(mp3.isDisposed()).isTrue();
              assertThat(mp3.peek().getT1()).isEqualTo(1);
              assertThat(mp3.peek().getT2()).isEqualTo(2);
            })
        .expectNextMatches(t -> t.getT1() == 1 && t.getT2() == 2)
        .verifyComplete();
  }

  @Test
  public void zipMonoProcessor2() {
    UnicastMonoProcessor<Integer> mp = UnicastMonoProcessor.create();
    UnicastMonoProcessor<Integer> mp3 = UnicastMonoProcessor.create();

    StepVerifier.create(Mono.zip(d -> (Integer) d[0], mp).subscribeWith(mp3))
        .then(() -> assertThat(mp3.isDisposed()).isFalse())
        .then(() -> mp.onNext(1))
        .then(
            () -> {
              assertThat(mp3.isDisposed()).isTrue();
              assertThat(mp3.peek()).isEqualTo(1);
            })
        .expectNext(1)
        .verifyComplete();
  }

  @Test
  public void zipMonoProcessorRejected() {
    UnicastMonoProcessor<Integer> mp = UnicastMonoProcessor.create();
    UnicastMonoProcessor<Integer> mp2 = UnicastMonoProcessor.create();
    UnicastMonoProcessor<Tuple2<Integer, Integer>> mp3 = UnicastMonoProcessor.create();

    StepVerifier.create(Mono.zip(mp, mp2).subscribeWith(mp3))
        .then(() -> assertThat(mp3.isDisposed()).isFalse())
        .then(() -> mp.onError(new Exception("test")))
        .then(
            () -> {
              assertThat(mp3.isDisposed()).isTrue();
              assertThat(mp3.getError()).hasMessage("test");
            })
        .verifyErrorMessage("test");
  }

  @Test
  public void filterMonoProcessor() {
    UnicastMonoProcessor<Integer> mp = UnicastMonoProcessor.create();
    UnicastMonoProcessor<Integer> mp2 = UnicastMonoProcessor.create();
    StepVerifier.create(mp.filter(s -> s % 2 == 0).subscribeWith(mp2))
        .then(() -> mp.onNext(2))
        .then(() -> assertThat(mp2.isError()).isFalse())
        .then(() -> assertThat(mp2.isDisposed()).isTrue())
        .then(() -> assertThat(mp2.peek()).isEqualTo(2))
        .then(() -> assertThat(mp2.isDisposed()).isTrue())
        .expectNext(2)
        .verifyComplete();
  }

  @Test
  public void filterMonoProcessorNot() {
    UnicastMonoProcessor<Integer> mp = UnicastMonoProcessor.create();
    UnicastMonoProcessor<Integer> mp2 = UnicastMonoProcessor.create();
    StepVerifier.create(mp.filter(s -> s % 2 == 0).subscribeWith(mp2))
        .then(() -> mp.onNext(1))
        .then(() -> assertThat(mp2.isError()).isFalse())
        .then(() -> assertThat(mp2.isDisposed()).isTrue())
        .then(() -> assertThat(mp2.peek()).isNull())
        .then(() -> assertThat(mp2.isDisposed()).isTrue())
        .verifyComplete();
  }

  @Test
  public void filterMonoProcessorError() {
    UnicastMonoProcessor<Integer> mp = UnicastMonoProcessor.create();
    UnicastMonoProcessor<Integer> mp2 = UnicastMonoProcessor.create();
    StepVerifier.create(
            mp.filter(
                    s -> {
                      throw new RuntimeException("test");
                    })
                .subscribeWith(mp2))
        .then(() -> mp.onNext(2))
        .then(() -> assertThat(mp2.isError()).isTrue())
        .then(() -> assertThat(mp2.isDisposed()).isTrue())
        .then(() -> assertThat(mp2.getError()).hasMessage("test"))
        .then(() -> assertThat(mp2.isDisposed()).isTrue())
        .verifyErrorMessage("test");
  }

  @Test
  public void doOnSuccessMonoProcessorError() {
    UnicastMonoProcessor<Integer> mp = UnicastMonoProcessor.create();
    UnicastMonoProcessor<Integer> mp2 = UnicastMonoProcessor.create();
    AtomicReference<Throwable> ref = new AtomicReference<>();

    StepVerifier.create(
            mp.doOnSuccess(
                    s -> {
                      throw new RuntimeException("test");
                    })
                .doOnError(ref::set)
                .subscribeWith(mp2))
        .then(() -> mp.onNext(2))
        .then(() -> assertThat(mp2.isError()).isTrue())
        .then(() -> assertThat(ref.get()).hasMessage("test"))
        .then(() -> assertThat(mp2.isDisposed()).isTrue())
        .then(() -> assertThat(mp2.getError()).hasMessage("test"))
        .then(() -> assertThat(mp2.isDisposed()).isTrue())
        .verifyErrorMessage("test");
  }

  @Test
  public void fluxCancelledByMonoProcessor() {
    AtomicLong cancelCounter = new AtomicLong();
    Flux.range(1, 10)
        .doOnCancel(cancelCounter::incrementAndGet)
        .transformDeferred((s) -> s.subscribeWith(UnicastMonoProcessor.create()))
        .subscribe();

    assertThat(cancelCounter.get()).isEqualTo(1);
  }

  @Test
  public void cancelledByMonoProcessor() {
    AtomicLong cancelCounter = new AtomicLong();
    UnicastMonoProcessor<String> monoProcessor =
        Mono.just("foo")
            .doOnCancel(cancelCounter::incrementAndGet)
            .subscribeWith(UnicastMonoProcessor.create());
    monoProcessor.subscribe();

    assertThat(cancelCounter.get()).isEqualTo(1);
  }

  @Test
  public void scanProcessor() {
    UnicastMonoProcessor<String> test = UnicastMonoProcessor.create();
    Subscription subscription = Operators.emptySubscription();
    test.onSubscribe(subscription);

    assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
    assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
    assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();

    test.onComplete();
    assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
    assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
  }

  @Test
  public void scanProcessorCancelled() {
    UnicastMonoProcessor<String> test = UnicastMonoProcessor.create();
    Subscription subscription = Operators.emptySubscription();
    test.onSubscribe(subscription);

    assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
    assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
    assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();

    test.cancel();
    assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
    assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
  }

  @Test
  public void scanProcessorSubscription() {
    UnicastMonoProcessor<String> test = UnicastMonoProcessor.create();
    Subscription subscription = Operators.emptySubscription();
    test.onSubscribe(subscription);

    assertThat(test.scan(Scannable.Attr.ACTUAL)).isNull();
    assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(subscription);
  }

  @Test
  public void scanProcessorError() {
    UnicastMonoProcessor<String> test = UnicastMonoProcessor.create();
    Subscription subscription = Operators.emptySubscription();
    test.onSubscribe(subscription);

    test.onError(new IllegalStateException("boom"));

    assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");
  }

  @Test
  public void monoToProcessorConnects() {
    TestPublisher<String> tp = TestPublisher.create();
    UnicastMonoProcessor<String> connectedProcessor =
        tp.mono().subscribeWith(UnicastMonoProcessor.create());

    assertThat(connectedProcessor.subscription).isNotNull();
  }

  @Test
  public void monoToProcessorChain() {
    StepVerifier.withVirtualTime(
            () ->
                Mono.just("foo")
                    .subscribeWith(UnicastMonoProcessor.create())
                    .delayElement(Duration.ofMillis(500)))
        .expectSubscription()
        .expectNoEvent(Duration.ofMillis(500))
        .expectNext("foo")
        .verifyComplete();
  }

  @Test
  public void monoToProcessorChainColdToHot() {
    AtomicInteger subscriptionCount = new AtomicInteger();
    Mono<String> coldToHot =
        Mono.just("foo")
            .doOnSubscribe(sub -> subscriptionCount.incrementAndGet())
            .transformDeferred(s -> s.subscribeWith(UnicastMonoProcessor.create()))
            .subscribeWith(UnicastMonoProcessor.create()) // this actually subscribes
            .filter(s -> s.length() < 4);

    assertThat(subscriptionCount.get()).isEqualTo(1);

    coldToHot.block();
    assertThatThrownBy(coldToHot::block)
        .hasMessage("UnicastMonoProcessor allows only a single Subscriber");
    assertThatThrownBy(coldToHot::block)
        .hasMessage("UnicastMonoProcessor allows only a single Subscriber");

    assertThat(subscriptionCount.get()).isEqualTo(1);
  }

  @Test
  public void monoProcessorBlockIsUnbounded() {
    long start = System.nanoTime();

    String result =
        Mono.just("foo")
            .delayElement(Duration.ofMillis(500))
            .subscribeWith(UnicastMonoProcessor.create())
            .block();

    assertThat(result).isEqualTo("foo");
    assertThat(Duration.ofNanos(System.nanoTime() - start))
        .isGreaterThanOrEqualTo(Duration.ofMillis(500));
  }

  @Test
  public void monoProcessorBlockNegativeIsImmediateTimeout() {
    long start = System.nanoTime();

    assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(
            () ->
                Mono.just("foo")
                    .delayElement(Duration.ofMillis(500))
                    .subscribeWith(UnicastMonoProcessor.create())
                    .block(Duration.ofSeconds(-1)))
        .withMessage("Timeout on blocking read for -1000 MILLISECONDS");

    assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofMillis(500));
  }

  @Test
  public void monoProcessorBlockZeroIsImmediateTimeout() {
    long start = System.nanoTime();

    assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(
            () ->
                Mono.just("foo")
                    .delayElement(Duration.ofMillis(500))
                    .subscribeWith(UnicastMonoProcessor.create())
                    .block(Duration.ZERO))
        .withMessage("Timeout on blocking read for 0 MILLISECONDS");

    assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofMillis(500));
  }

  @Test
  public void disposeBeforeValueSendsCancellationException() {
    UnicastMonoProcessor<String> processor = UnicastMonoProcessor.create();
    AtomicReference<Throwable> e1 = new AtomicReference<>();
    AtomicReference<Throwable> e2 = new AtomicReference<>();
    AtomicReference<Throwable> e3 = new AtomicReference<>();
    AtomicReference<Throwable> late = new AtomicReference<>();

    processor.subscribe(v -> Assertions.fail("expected first subscriber to error"), e1::set);
    processor.subscribe(v -> Assertions.fail("expected second subscriber to error"), e2::set);
    processor.subscribe(v -> Assertions.fail("expected third subscriber to error"), e3::set);

    processor.dispose();

    assertThat(e1.get()).isInstanceOf(CancellationException.class);
    assertThat(e2.get()).isInstanceOf(IllegalStateException.class);
    assertThat(e3.get()).isInstanceOf(IllegalStateException.class);

    processor.subscribe(v -> Assertions.fail("expected late subscriber to error"), late::set);
    assertThat(late.get()).isInstanceOf(IllegalStateException.class);
  }
}
