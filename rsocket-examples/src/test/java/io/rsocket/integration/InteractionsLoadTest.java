package io.rsocket.integration;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
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

public class InteractionsLoadTest {

  @Test
  @SlowTest
  public void channel() {
    CloseableChannel server =
        RSocketServer.create(SocketAcceptor.with(new EchoRSocket()))
            .bind(TcpServerTransport.create("localhost", 0))
            .block(Duration.ofSeconds(10));

    RSocket clientRSocket =
        RSocketConnector.connectWith(TcpClientTransport.create(server.address()))
            .block(Duration.ofSeconds(10));

    int concurrency = 16;
    Flux.range(1, concurrency)
        .flatMap(
            v ->
                clientRSocket
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

  private static class EchoRSocket implements RSocket {

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
