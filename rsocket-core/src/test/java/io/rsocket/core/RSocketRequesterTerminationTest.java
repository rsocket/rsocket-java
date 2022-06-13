package io.rsocket.core;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketRequesterTest.ClientSocketRule;
import io.rsocket.util.EmptyPayload;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.Arrays;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class RSocketRequesterTerminationTest {

  public final ClientSocketRule rule = new ClientSocketRule();

  @BeforeEach
  public void setup() {
    rule.init();
  }

  @ParameterizedTest
  @MethodSource("rsocketInteractions")
  public void testCurrentStreamIsTerminatedOnConnectionClose(
      Function<RSocket, ? extends Publisher<?>> interaction) {
    RSocketRequester rSocket = rule.socket;

    Mono.delay(Duration.ofSeconds(1)).doOnNext(v -> rule.connection.dispose()).subscribe();

    StepVerifier.create(interaction.apply(rSocket))
        .expectError(ClosedChannelException.class)
        .verify(Duration.ofSeconds(5));
  }

  @ParameterizedTest
  @MethodSource("rsocketInteractions")
  public void testSubsequentStreamIsTerminatedAfterConnectionClose(
      Function<RSocket, ? extends Publisher<?>> interaction) {
    RSocketRequester rSocket = rule.socket;

    rule.connection.dispose();
    StepVerifier.create(interaction.apply(rSocket))
        .expectError(ClosedChannelException.class)
        .verify(Duration.ofSeconds(5));
  }

    @Test
    void disposingShouldDisposeOfIncomingFramesDisposable() {
        RSocketRequester rSocketRequester = rule.socket;
        assertThat(rSocketRequester.isDisposed()).isFalse();

        rSocketRequester.dispose();

        assertThat(rSocketRequester.isDisposed()).isTrue();
        assertThat(rSocketRequester.getIncomingFramesDisposable().isDisposed()).isTrue();
    }

  public static Iterable<Function<RSocket, ? extends Publisher<?>>> rsocketInteractions() {
    EmptyPayload payload = EmptyPayload.INSTANCE;
    Publisher<Payload> payloadStream = Flux.just(payload);

    Function<RSocket, Mono<Payload>> resp =
        new Function<RSocket, Mono<Payload>>() {
          @Override
          public Mono<Payload> apply(RSocket rSocket) {
            return rSocket.requestResponse(payload);
          }

          @Override
          public String toString() {
            return "Request Response";
          }
        };
    Function<RSocket, Flux<Payload>> stream =
        new Function<RSocket, Flux<Payload>>() {
          @Override
          public Flux<Payload> apply(RSocket rSocket) {
            return rSocket.requestStream(payload);
          }

          @Override
          public String toString() {
            return "Request Stream";
          }
        };
    Function<RSocket, Flux<Payload>> channel =
        new Function<RSocket, Flux<Payload>>() {
          @Override
          public Flux<Payload> apply(RSocket rSocket) {
            return rSocket.requestChannel(payloadStream);
          }

          @Override
          public String toString() {
            return "Request Channel";
          }
        };

    return Arrays.asList(resp, stream, channel);
  }
}
