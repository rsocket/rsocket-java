package io.rsocket.internal;

import java.time.Duration;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

public class SwitchTransformFluxTest {

  @Test
  public void backpressureTest() {
    TestPublisher<Long> publisher = TestPublisher.createCold();

    Flux<String> switchTransformed =
        publisher
            .flux()
            .transform(
                flux ->
                    new SwitchTransformFlux<>(
                        flux, (first, innerFlux) -> innerFlux.map(String::valueOf)));

    publisher.next(1L);

    StepVerifier.create(switchTransformed, 0)
        .thenRequest(1)
        .expectNext("1")
        .thenRequest(1)
        .then(() -> publisher.next(2L))
        .expectNext("2")
        .then(publisher::complete)
        .expectComplete()
        .verify(Duration.ofSeconds(10));

    publisher.assertWasRequested();
    publisher.assertNoRequestOverflow();
  }

  @Test
  public void shouldErrorOnOverflowTest() {
    TestPublisher<Long> publisher = TestPublisher.createCold();

    Flux<String> switchTransformed =
        publisher
            .flux()
            .transform(
                flux ->
                    new SwitchTransformFlux<>(
                        flux, (first, innerFlux) -> innerFlux.map(String::valueOf)));

    publisher.next(1L);

    StepVerifier.create(switchTransformed, 0)
        .thenRequest(1)
        .expectNext("1")
        .then(() -> publisher.next(2L))
        .expectError()
        .verify(Duration.ofSeconds(10));

    publisher.assertWasRequested();
    publisher.assertNoRequestOverflow();
  }

  @Test
  public void shouldPropagateonCompleteCorrectly() {
    Flux<String> switchTransformed =
        Flux.empty()
            .transform(
                flux ->
                    new SwitchTransformFlux<>(
                        flux, (first, innerFlux) -> innerFlux.map(String::valueOf)));

    StepVerifier.create(switchTransformed).expectComplete().verify(Duration.ofSeconds(10));
  }

  @Test
  public void shouldPropagateErrorCorrectly() {
    Flux<String> switchTransformed =
        Flux.error(new RuntimeException("hello"))
            .transform(
                flux ->
                    new SwitchTransformFlux<>(
                        flux, (first, innerFlux) -> innerFlux.map(String::valueOf)));

    StepVerifier.create(switchTransformed)
        .expectErrorMessage("hello")
        .verify(Duration.ofSeconds(10));
  }

  @Test
  public void shouldBeAbleToBeCancelledProperly() {
    TestPublisher<Integer> publisher = TestPublisher.createCold();
    Flux<String> switchTransformed =
        publisher
            .flux()
            .transform(
                flux ->
                    new SwitchTransformFlux<>(
                        flux, (first, innerFlux) -> innerFlux.map(String::valueOf)));

    publisher.emit(1, 2, 3, 4, 5);

    StepVerifier.create(switchTransformed, 0).thenCancel().verify(Duration.ofSeconds(10));

    publisher.assertCancelled();
    publisher.assertWasRequested();
  }
}
