package io.rsocket.transport.netty;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.rsocket.transport.netty.server.WebsocketRouteTransport;
import io.rsocket.util.DefaultPayload;
import io.rsocket.util.EmptyPayload;
import java.net.URI;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import reactor.test.StepVerifier;

public class WebSocketTransportIntegrationTest {

  @Test
  public void sendStreamOfDataWithExternalHttpServerTest() {
    ServerTransport.ConnectionAcceptor acceptor =
        RSocketFactory.receive()
            .acceptor(
                (setupPayload, sendingRSocket) -> {
                  return Mono.just(
                      new AbstractRSocket() {
                        @Override
                        public Flux<Payload> requestStream(Payload payload) {
                          return Flux.range(0, 10)
                              .map(i -> DefaultPayload.create(String.valueOf(i)));
                        }
                      });
                })
            .toConnectionAcceptor();

    DisposableServer server =
        HttpServer.create()
            .host("localhost")
            .route(router -> router.ws("/test", WebsocketRouteTransport.newHandler(acceptor)))
            .bindNow();

    RSocket rsocket =
        RSocketFactory.connect()
            .transport(
                WebsocketClientTransport.create(
                    URI.create("ws://" + server.host() + ":" + server.port() + "/test")))
            .start()
            .block();

    StepVerifier.create(rsocket.requestStream(EmptyPayload.INSTANCE))
        .expectSubscription()
        .expectNextCount(10)
        .expectComplete()
        .verify(Duration.ofMillis(1000));
  }
}
