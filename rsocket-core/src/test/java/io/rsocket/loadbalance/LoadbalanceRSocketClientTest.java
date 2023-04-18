package io.rsocket.loadbalance;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketClient;
import io.rsocket.core.RSocketConnector;
import io.rsocket.transport.ClientTransport;
import io.rsocket.util.DefaultPayload;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class LoadbalanceRSocketClientTest {

  @Mock private ClientTransport clientTransport;
  @Mock private RSocketConnector rSocketConnector;

  public static final Duration SHORT_DURATION = Duration.ofMillis(25);
  public static final Duration LONG_DURATION = Duration.ofMillis(75);

  private static final Publisher<Payload> SOURCE =
      Flux.interval(SHORT_DURATION)
          .onBackpressureBuffer()
          .map(String::valueOf)
          .map(DefaultPayload::create);

  private static final Mono<RSocket> PROGRESSING_HANDLER =
      Mono.just(
          new RSocket() {
            private final AtomicInteger i = new AtomicInteger();

            @Override
            public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
              return Flux.from(payloads)
                  .delayElements(SHORT_DURATION)
                  .map(Payload::getDataUtf8)
                  .map(DefaultPayload::create)
                  .take(i.incrementAndGet());
            }
          });

  @Test
  void testChannelReconnection() {
    when(rSocketConnector.connect(clientTransport)).thenReturn(PROGRESSING_HANDLER);

    RSocketClient client =
        LoadbalanceRSocketClient.create(
            rSocketConnector,
            Mono.just(singletonList(LoadbalanceTarget.from("key", clientTransport))));

    Publisher<String> result =
        client
            .requestChannel(SOURCE)
            .repeatWhen(longFlux -> longFlux.delayElements(LONG_DURATION).take(5))
            .map(Payload::getDataUtf8)
            .log();

    StepVerifier.create(result)
        .expectSubscription()
        .assertNext(s -> assertThat(s).isEqualTo("0"))
        .assertNext(s -> assertThat(s).isEqualTo("0"))
        .assertNext(s -> assertThat(s).isEqualTo("1"))
        .assertNext(s -> assertThat(s).isEqualTo("0"))
        .assertNext(s -> assertThat(s).isEqualTo("1"))
        .assertNext(s -> assertThat(s).isEqualTo("2"))
        .assertNext(s -> assertThat(s).isEqualTo("0"))
        .assertNext(s -> assertThat(s).isEqualTo("1"))
        .assertNext(s -> assertThat(s).isEqualTo("2"))
        .assertNext(s -> assertThat(s).isEqualTo("3"))
        .assertNext(s -> assertThat(s).isEqualTo("0"))
        .assertNext(s -> assertThat(s).isEqualTo("1"))
        .assertNext(s -> assertThat(s).isEqualTo("2"))
        .assertNext(s -> assertThat(s).isEqualTo("3"))
        .assertNext(s -> assertThat(s).isEqualTo("4"))
        .verifyComplete();

    verify(rSocketConnector).connect(clientTransport);
    verifyNoMoreInteractions(rSocketConnector, clientTransport);
  }
}
