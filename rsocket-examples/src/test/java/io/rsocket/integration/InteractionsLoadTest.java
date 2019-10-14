package io.rsocket.integration;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.test.SlowTest;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import java.time.Duration;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class InteractionsLoadTest {

  @Test
  @SlowTest
  public void channel() {
    TcpServerTransport serverTransport = TcpServerTransport.create(0);

    CloseableChannel server =
        RSocketFactory.receive()
            .acceptor((setup, rsocket) -> Mono.just(new EchoRSocket()))
            .transport(serverTransport)
            .start()
            .block(Duration.ofSeconds(10));

    TcpClientTransport transport = TcpClientTransport.create(server.address());

    RSocket client =
        RSocketFactory.connect().transport(transport).start().block(Duration.ofSeconds(10));

    int concurrency = 16;
    Flux.range(1, concurrency)
        .flatMap(
            v ->
                client
                    .requestChannel(
                        input().onBackpressureDrop().map(iv -> DefaultPayload.create("foo")))
                    .limitRate(10000),
            concurrency)
        .timeout(Duration.ofSeconds(5))
        .doOnNext(
            p -> {
              String data = p.getDataUtf8();
              if (!data.equals("bar")) {
                throw new IllegalStateException("Channel Client Bad message: " + data);
              }
            })
        .window(Duration.ofSeconds(1))
        .flatMap(Flux::count)
        .doOnNext(d -> System.out.println("Got: " + d))
        .take(Duration.ofMinutes(1))
        .doOnTerminate(server::dispose)
        .subscribe();

    server.onClose().block();
  }

  private static Flux<Long> input() {
    Flux<Long> interval = Flux.interval(Duration.ofMillis(1)).onBackpressureDrop();
    for (int i = 0; i < 10; i++) {
      interval = interval.mergeWith(interval);
    }
    return interval;
  }

  private static class EchoRSocket extends AbstractRSocket {
    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
      return Flux.from(payloads)
          .map(
              p -> {
                String data = p.getDataUtf8();
                if (!data.equals("foo")) {
                  throw new IllegalStateException("Channel Server Bad message: " + data);
                }
                return DefaultPayload.create("bar");
              });
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
      return Flux.just(payload)
          .map(
              p -> {
                String data = p.getDataUtf8();
                return data;
              })
          .doOnNext(
              (data) -> {
                if (!data.equals("foo")) {
                  throw new IllegalStateException("Stream Server Bad message: " + data);
                }
              })
          .flatMap(
              data -> {
                Supplier<Payload> p = () -> DefaultPayload.create("bar");
                return Flux.range(1, 100).map(v -> p.get());
              });
    }
  }
}
