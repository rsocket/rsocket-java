package io.rsocket;

import io.rsocket.RSocketRequesterTest.ClientSocketRule;
import io.rsocket.util.EmptyPayload;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.Arrays;
import java.util.function.Function;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@RunWith(Parameterized.class)
public class RSocketRequesterTerminationTest {

  @Rule public final ClientSocketRule rule = new ClientSocketRule();
  private Function<RSocket, ? extends Publisher<?>> interaction;

  public RSocketRequesterTerminationTest(Function<RSocket, ? extends Publisher<?>> interaction) {
    this.interaction = interaction;
  }

  @Test
  public void testCurrentStreamIsTerminatedOnConnectionClose() {
    RSocketRequester rSocket = rule.socket;

    Mono.delay(Duration.ofSeconds(1)).doOnNext(v -> rule.connection.dispose()).subscribe();

    StepVerifier.create(interaction.apply(rSocket))
        .expectError(ClosedChannelException.class)
        .verify(Duration.ofSeconds(5));
  }

  @Test
  public void testSubsequentStreamIsTerminatedAfterConnectionClose() {
    RSocketRequester rSocket = rule.socket;

    rule.connection.dispose();
    StepVerifier.create(interaction.apply(rSocket))
        .expectError(ClosedChannelException.class)
        .verify(Duration.ofSeconds(5));
  }

  @Parameterized.Parameters
  public static Iterable<Function<RSocket, ? extends Publisher<?>>> rsocketInteractions() {
    EmptyPayload payload = EmptyPayload.INSTANCE;
    Publisher<Payload> payloadStream = Flux.just(payload);

    Function<RSocket, Mono<Payload>> resp = rSocket -> rSocket.requestResponse(payload);
    Function<RSocket, Flux<Payload>> stream = rSocket -> rSocket.requestStream(payload);
    Function<RSocket, Flux<Payload>> channel = rSocket -> rSocket.requestChannel(payloadStream);

    return Arrays.asList(resp, stream, channel);
  }
}
