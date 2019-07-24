package io.rsocket.internal;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

class RateLimitableRequestPublisherTest {

  @Test
  public void testThatRequest1WillBePropagatedUpstream() {
    Flux<Integer> source =
        Flux.just(1)
            .subscribeOn(Schedulers.parallel())
            .doOnRequest(r -> Assertions.assertThat(r).isLessThanOrEqualTo(128));

    RateLimitableRequestPublisher<Integer> rateLimitableRequestPublisher =
        RateLimitableRequestPublisher.wrap(source, 128);

    StepVerifier.create(rateLimitableRequestPublisher)
        .then(() -> rateLimitableRequestPublisher.request(1))
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

    RateLimitableRequestPublisher<Integer> rateLimitableRequestPublisher =
        RateLimitableRequestPublisher.wrap(source, 128);

    StepVerifier.create(rateLimitableRequestPublisher)
        .then(() -> rateLimitableRequestPublisher.request(256))
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

    RateLimitableRequestPublisher<Integer> rateLimitableRequestPublisher =
        RateLimitableRequestPublisher.wrap(source, 128);

    StepVerifier.create(rateLimitableRequestPublisher)
        .then(() -> rateLimitableRequestPublisher.request(10))
        .expectNextCount(5)
        .then(() -> rateLimitableRequestPublisher.request(128))
        .expectNextCount(133)
        .expectNoEvent(Duration.ofMillis(10))
        .then(() -> rateLimitableRequestPublisher.request(Long.MAX_VALUE))
        .expectNextCount(118)
        .expectComplete()
        .verify(Duration.ofMillis(1000));
  }

  @Test
  public void testThatRequestInRandomFashionWillBePropagatedToUpstreamWithLimitedRateInFewSteps() {
    Flux<Integer> source =
        Flux.range(0, 10000000)
            .subscribeOn(Schedulers.parallel())
            .doOnRequest(r -> Assertions.assertThat(r).isLessThanOrEqualTo(128));

    RateLimitableRequestPublisher<Integer> rateLimitableRequestPublisher =
        RateLimitableRequestPublisher.wrap(source, 128);

    StepVerifier.create(rateLimitableRequestPublisher)
        .then(
            () ->
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

                            rateLimitableRequestPublisher.request(count);
                          }
                        }))
        .expectNextCount(10000000)
        .expectComplete()
        .verify(Duration.ofMillis(30000));
  }
}
