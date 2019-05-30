package io.rsocket.fragmentation;

import io.netty.util.ResourceLeakDetector;
import io.rsocket.*;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import java.time.Duration;
import java.util.Random;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class FragmentationDebug {
  private static final int PAYLOADS_PER_STREAM_COUNT = 10;
  private static final int ITERATIONS_COUNT = 1000;

  static {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  public static void main(String[] args) {
    CloseableChannel server =
        RSocketFactory.receive()
            .errorConsumer(err -> System.out.println(err.getMessage()))
            .fragment(100)
            .acceptor(new SocketAcceptorImpl())
            .transport(TcpServerTransport.create("localhost", 7000))
            .start()
            .block();

    RSocket socket =
        RSocketFactory.connect()
            .errorConsumer(err -> System.out.println(err.getMessage()))
            .fragment(100)
            .transport(TcpClientTransport.create(server.address()))
            .start()
            .block();
    byte[] data = new byte[164];
    byte[] metadata = new byte[164];
    Random random = new Random();
    random.nextBytes(data);
    random.nextBytes(metadata);

    for (int i = 0; i < ITERATIONS_COUNT; i++) {
      System.out.println("Iteration: " + (i + 1));
      Flux<Payload> result =
          socket.requestChannel(
              Flux.range(1, PAYLOADS_PER_STREAM_COUNT)
                  .map(
                      idx -> {
                        return DefaultPayload.create(data, metadata);
                      }));
      StepVerifier.create(result)
          .expectNextCount(PAYLOADS_PER_STREAM_COUNT)
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
