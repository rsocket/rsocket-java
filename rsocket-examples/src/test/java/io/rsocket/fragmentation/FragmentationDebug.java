package io.rsocket.fragmentation;

import io.netty.util.ResourceLeakDetector;
import io.rsocket.*;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.WebsocketServerTransport;
import io.rsocket.util.DefaultPayload;
import java.time.Duration;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class FragmentationDebug {
  static {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  public static void main(String[] args) {
    CloseableChannel server =
        RSocketFactory.receive()
            .errorConsumer(err -> System.out.println(err.getMessage()))
            .fragment(100)
            .acceptor(new SocketAcceptorImpl())
            .transport(WebsocketServerTransport.create("localhost", 7000))
            .start()
            .block();

    RSocket socket =
        RSocketFactory.connect()
            .errorConsumer(err -> System.out.println(err.getMessage()))
            .fragment(100)
            .transport(WebsocketClientTransport.create(server.address()))
            .start()
            .block();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      sb.append("Hello hello hello []");
    }
    String data = sb.toString();
    for (int i = 0; i < 1000; i++) {
      System.out.println("Iteration: " + (i + 1));
      Flux<String> result =
          socket
              .requestChannel(
                  Flux.range(1, 10)
                      .map(
                          idx -> {
                            return DefaultPayload.create(data, data);
                          }))
              .map(Payload::getDataUtf8)
              .log();
      String expected = "Echo: " + data;
      StepVerifier.create(result)
          .expectNextCount(10)
          .expectComplete()
          .verify(Duration.ofSeconds(5));
    }
  }

  private static class SocketAcceptorImpl implements SocketAcceptor {
    @Override
    public Mono<RSocket> accept(ConnectionSetupPayload setupPayload, RSocket reactiveSocket) {
      return Mono.just(
          new AbstractRSocket() {
            @Override
            public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
              return Flux.from(payloads)
                  .delayElements(Duration.ofMillis(10))
                  .map(Payload::getDataUtf8)
                  .map(s -> "Echo: " + s)
                  .map(data -> DefaultPayload.create(data, data));
            }
          });
    }
  }
}
