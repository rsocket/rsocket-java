package io.rsocket;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class RSocketClientTerminationTest {
/*
  @Rule
  public final ClientSocketRule rule = new ClientSocketRule();
  private Function<RSocket, ? extends Publisher<?>> interaction;

  public RSocketClientTerminationTest(Function<RSocket, ? extends Publisher<?>> interaction) {
    this.interaction = interaction;
  }

  @Test
  public void testCurrentStreamIsTerminatedOnConnectionClose() {
    RSocketClient rSocket = rule.socket;

    Mono.delay(Duration.ofSeconds(1))
        .doOnNext(v -> rule.connection.dispose())
        .subscribe();

    StepVerifier.create(interaction.apply(rSocket))
        .expectError(ClosedChannelException.class)
        .verify(Duration.ofSeconds(5));
  }

  @Test
  public void testSubsequentStreamIsTerminatedAfterConnectionClose() {
    RSocketClient rSocket = rule.socket;

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
  }*/
}
