package io.rsocket.integration;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.NettyContextCloseable;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.function.Supplier;

public class InteractionsLoadTest {

  public static void main(String[] args) {
    TcpServerTransport serverTransport = TcpServerTransport.create(0);

    NettyContextCloseable server = RSocketFactory.receive()
       // .frameDecoder(ByteBufPayload::create)
        .acceptor((setup, rsocket) -> Mono.just(new EchoRSocket()))
        .transport(serverTransport)
        .start()
        .block(Duration.ofSeconds(10));

    TcpClientTransport transport = TcpClientTransport.create(server.address());

    RSocket client = RSocketFactory
        .connect()
        //.frameDecoder(ByteBufPayload::create)
        .transport(transport).start()
        .block(Duration.ofSeconds(10));

    int concurrency = 16;
    Flux.range(1, concurrency)
        .flatMap(v ->
            client.requestChannel(
                input().map(iv ->
                DefaultPayload.create("foo")))
                .limitRate(10000),concurrency)
        .doOnNext(p -> {
          String data = p.getDataUtf8();
          if (!data.equals("bar")) {
            System.out.println("Channel Client Bad message: " + data);
          }
          //p.release();
        })
        .window(Duration.ofSeconds(1))
        .flatMap(Flux::count)
        .doOnNext(d -> System.out.println("Got: " + d))
        .timeout(Duration.ofSeconds(525))
        .take(Duration.ofMinutes(2))
        .doOnTerminate(server::dispose)
        .subscribe();

    server.onClose().block();

  }

  private static Flux<Long> input() {
    Flux<Long> interval = Flux.interval(Duration.ofMillis(1))
        .onBackpressureDrop();
    for (int i = 0; i < 10; i++) {
      interval = interval.mergeWith(interval);
    }
    return interval.publishOn(Schedulers.newSingle("bar"));
  }

  private static class EchoRSocket extends AbstractRSocket {
    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
      return Flux.from(payloads).map(p -> {

        String data = p.getDataUtf8();
        if (!data.equals("foo")) {
          System.out.println("Channel Server Bad message: " + data);
        }
       // p.release();
        return DefaultPayload.create(DefaultPayload.create("bar"));
      });
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
      return Flux.just(payload)
          .map(p -> {
            String data = p.getDataUtf8();
          //  p.release();
            return data;
          })
          .doOnNext((data) -> {
            if (!data.equals("foo")) {
              System.out.println("Stream Server Bad message: " + data);
            }
          }).flatMap(data -> {
            Supplier<Payload> p = () -> DefaultPayload.create("bar");
            return Flux.range(1, 100).map(v -> p.get());
          });
    }
  }
}
