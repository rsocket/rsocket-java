package io.rsocket.internal;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

class RateLimitableRequestPublisherTest {

  @Test
  public void testThatRequest1WillBePropagatedUpstream() {
    Flux<Integer> source =
        Flux.just(1)
            .subscribeOn(Schedulers.parallel())
            .doOnRequest(r -> Assertions.assertThat(r).isLessThanOrEqualTo(128));

    StepVerifier.create(
            source.transform(
                Operators.<Integer, Integer>lift(
                    (__, actual) ->
                        new RateLimitableRequestSubscriber<Integer>(128) {
                          @Override
                          public void hookOnSubscribe(Subscription s) {
                            actual.onSubscribe(this);
                          }

                          @Override
                          public void hookOnNext(Integer o) {
                            actual.onNext(o);
                          }

                          @Override
                          public void hookOnError(Throwable t) {
                            actual.onError(t);
                          }

                          @Override
                          public void hookOnComplete() {
                            actual.onComplete();
                          }
                        })))
        .expectNext(1)
        .expectComplete()
        .verify(Duration.ofMillis(1000));
  }

  @Test
  public void testThatRequest256WillBePropagatedToUpstreamWithLimitedRate() {
    Flux<Integer> source =
        Flux.range(0, 256)
            .subscribeOn(Schedulers.parallel())
            .doOnRequest(r -> Assertions.assertThat(r).isLessThanOrEqualTo(128));

    StepVerifier.create(
            source.transform(
                Operators.<Integer, Integer>lift(
                    (__, actual) ->
                        new RateLimitableRequestSubscriber<Integer>(128) {
                          @Override
                          public void hookOnSubscribe(Subscription s) {
                            actual.onSubscribe(this);
                          }

                          @Override
                          public void hookOnNext(Integer o) {
                            actual.onNext(o);
                          }

                          @Override
                          public void hookOnError(Throwable t) {
                            actual.onError(t);
                          }

                          @Override
                          public void hookOnComplete() {
                            actual.onComplete();
                          }
                        })))
        .expectNextCount(256)
        .expectComplete()
        .verify(Duration.ofMillis(1000));
  }

  @Test
  public void testThatRequest256WillBePropagatedToUpstreamWithLimitedRateInFewSteps() {
    Flux<Integer> source =
        Flux.range(0, 256)
            .subscribeOn(Schedulers.parallel())
            .doOnRequest(r -> Assertions.assertThat(r).isLessThanOrEqualTo(128));

    StepVerifier.create(
            source.transform(
                Operators.<Integer, Integer>lift(
                    (__, actual) ->
                        new RateLimitableRequestSubscriber<Integer>(128) {
                          @Override
                          public void hookOnSubscribe(Subscription s) {
                            actual.onSubscribe(this);
                          }

                          @Override
                          public void hookOnNext(Integer o) {
                            actual.onNext(o);
                          }

                          @Override
                          public void hookOnError(Throwable t) {
                            actual.onError(t);
                          }

                          @Override
                          public void hookOnComplete() {
                            actual.onComplete();
                          }
                        })),
            0)
        .thenRequest(10)
        .expectNextCount(5)
        .thenRequest(128)
        .expectNextCount(133)
        .expectNoEvent(Duration.ofMillis(10))
        .thenRequest(Long.MAX_VALUE)
        .expectNextCount(118)
        .expectComplete()
        .verify(Duration.ofMillis(1000));
  }

  @Test
  public void testThatRequestInRandomFashionWillBePropagatedToUpstreamWithLimitedRateInFewSteps()
      throws InterruptedException {
    Flux<Integer> source =
        Flux.range(0, 10000000)
            .subscribeOn(Schedulers.parallel())
            .doOnRequest(r -> Assertions.assertThat(r).isLessThanOrEqualTo(128));

    CountDownLatch latch = new CountDownLatch(10000000 + 1);

    Subscription subscription =
        source.subscribeWith(
            new RateLimitableRequestSubscriber<Integer>(128) {
              @Override
              public void hookOnNext(Integer o) {
                latch.countDown();
              }

              @Override
              protected void hookFinally(SignalType type) {
                latch.countDown();
              }
            });
    Flux.interval(Duration.ofMillis(1000))
        .onBackpressureDrop()
        .subscribe(
            new Consumer<Long>() {
              int count = 10000000;

              @Override
              public void accept(Long __) {
                int random = ThreadLocalRandom.current().nextInt(1, 512);

                long request = Math.min(random, count);

                count -= request;

                subscription.request(count);
              }
            });
    Assertions.assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  public void testThatRequestLongMaxValueWillBeDeliveredInSeparateChunks() {
    Flux<Integer> source =
        Flux.range(0, 10000000)
            .subscribeOn(Schedulers.parallel())
            .doOnRequest(r -> Assertions.assertThat(r).isLessThanOrEqualTo(128));

    StepVerifier.create(
            source.transform(
                Operators.<Integer, Integer>lift(
                    (__, actual) ->
                        new RateLimitableRequestSubscriber<Integer>(128) {
                          @Override
                          public void hookOnSubscribe(Subscription s) {
                            actual.onSubscribe(this);
                          }

                          @Override
                          public void hookOnNext(Integer o) {
                            actual.onNext(o);
                          }

                          @Override
                          public void hookOnError(Throwable t) {
                            actual.onError(t);
                          }

                          @Override
                          public void hookOnComplete() {
                            actual.onComplete();
                          }
                        })))
        .expectNextCount(10000000)
        .expectComplete()
        .verify(Duration.ofMillis(30000));
  }

  @Test
  public void testThatRequestLongMaxWithIntegerMaxValuePrefetchWillBeDeliveredAsLongMaxValue() {
    Flux<Integer> source =
        Flux.range(0, 10000000)
            .subscribeOn(Schedulers.parallel())
            .doOnRequest(r -> Assertions.assertThat(r).isEqualTo(Long.MAX_VALUE));

    StepVerifier.create(
            source.transform(
                Operators.<Integer, Integer>lift(
                    (__, actual) ->
                        new RateLimitableRequestSubscriber<Integer>(128) {
                          @Override
                          public void hookOnSubscribe(Subscription s) {
                            actual.onSubscribe(this);
                          }

                          @Override
                          public void hookOnNext(Integer o) {
                            actual.onNext(o);
                          }

                          @Override
                          public void hookOnError(Throwable t) {
                            actual.onError(t);
                          }

                          @Override
                          public void hookOnComplete() {
                            actual.onComplete();
                          }
                        })))
        .expectNextCount(10000000)
        .expectComplete()
        .verify(Duration.ofMillis(30000));
  }
}
