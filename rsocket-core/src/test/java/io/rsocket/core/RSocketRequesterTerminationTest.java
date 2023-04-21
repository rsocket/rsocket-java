package io.rsocket.core;

import io.rsocket.FrameAssert;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketRequesterTest.ClientSocketRule;
import io.rsocket.frame.FrameType;
import io.rsocket.util.EmptyPayload;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.Arrays;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class RSocketRequesterTerminationTest {

  public final ClientSocketRule rule = new ClientSocketRule();

  @BeforeEach
  public void setup() {
    rule.init();
  }

  @AfterEach
  public void tearDownAndCheckNoLeaks() {
    rule.assertHasNoLeaks();
  }

  @ParameterizedTest
  @MethodSource("rsocketInteractions")
  public void testCurrentStreamIsTerminatedOnConnectionClose(
      FrameType requestType, Function<RSocket, ? extends Publisher<?>> interaction) {
    RSocketRequester rSocket = rule.socket;

    StepVerifier.create(interaction.apply(rSocket))
        .then(
            () -> {
              FrameAssert.assertThat(rule.connection.pollFrame()).typeOf(requestType).hasNoLeaks();
            })
        .then(() -> rule.connection.dispose())
        .expectError(ClosedChannelException.class)
        .verify(Duration.ofSeconds(5));
  }

  @ParameterizedTest
  @MethodSource("rsocketInteractions")
  public void testSubsequentStreamIsTerminatedAfterConnectionClose(
      FrameType requestType, Function<RSocket, ? extends Publisher<?>> interaction) {
    RSocketRequester rSocket = rule.socket;

    rule.connection.dispose();
    StepVerifier.create(interaction.apply(rSocket))
        .expectError(ClosedChannelException.class)
        .verify(Duration.ofSeconds(5));
  }

  public static Iterable<Arguments> rsocketInteractions() {
    EmptyPayload payload = EmptyPayload.INSTANCE;

    Arguments resp =
        Arguments.of(
            FrameType.REQUEST_RESPONSE,
            new Function<RSocket, Mono<Payload>>() {
              @Override
              public Mono<Payload> apply(RSocket rSocket) {
                return rSocket.requestResponse(payload);
              }

              @Override
              public String toString() {
                return "Request Response";
              }
            });
    Arguments stream =
        Arguments.of(
            FrameType.REQUEST_STREAM,
            new Function<RSocket, Flux<Payload>>() {
              @Override
              public Flux<Payload> apply(RSocket rSocket) {
                return rSocket.requestStream(payload);
              }

              @Override
              public String toString() {
                return "Request Stream";
              }
            });
    Arguments channel =
        Arguments.of(
            FrameType.REQUEST_CHANNEL,
            new Function<RSocket, Flux<Payload>>() {
              @Override
              public Flux<Payload> apply(RSocket rSocket) {
                return rSocket.requestChannel(Flux.<Payload>never().startWith(payload));
              }

              @Override
              public String toString() {
                return "Request Channel";
              }
            });

    return Arrays.asList(resp, stream, channel);
  }
}
