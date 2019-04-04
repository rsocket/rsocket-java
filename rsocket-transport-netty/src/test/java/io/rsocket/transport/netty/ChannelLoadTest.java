package io.rsocket.transport.netty;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.resume.InMemoryResumableFramesStore;
import io.rsocket.resume.PeriodicResumeStrategy;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class ChannelLoadTest {

  @Test
  void channel() {
    CloseableChannel closeableChannel = newServerRSocket().block();
    RSocket rSocket = newClientRSocket(closeableChannel.address(), err -> {
      throw new RuntimeException(err);
    }).block();

    StepVerifier.create(
        rSocket
            .requestChannel(testRequest())
            .take(Duration.ofSeconds(600))
            .map(Payload::getDataUtf8)
            .timeout(Duration.ofSeconds(5))
            .then()
            .doFinally(s -> closeableChannel.dispose()))
        .expectComplete()
        .verify();
  }

  private static Flux<Payload> testRequest() {
    return Flux.interval(Duration.ofMillis(50))
        .map(v -> DefaultPayload.create("client_request"))
        .onBackpressureDrop();
  }

  private static Mono<RSocket> newClientRSocket(InetSocketAddress address,
      Consumer<Throwable> errConsumer) {
    return RSocketFactory.connect()
        .keepAliveTickPeriod(Duration.ofSeconds(30))
        .keepAliveAckTimeout(Duration.ofMinutes(5))
        .errorConsumer(errConsumer)
        .transport(TcpClientTransport.create(address))
        .start();
  }

  private static Mono<CloseableChannel> newServerRSocket() {
    return RSocketFactory.receive()
        .acceptor((setupPayload, rSocket) -> Mono.just(new TestResponderRSocket()))
        .transport(TcpServerTransport.create("localhost", 0))
        .start();
  }

  private static class TestResponderRSocket extends AbstractRSocket {

    AtomicInteger counter = new AtomicInteger();

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
      return duplicate(
          Flux.interval(Duration.ofMillis(1))
              .onBackpressureLatest()
              .publishOn(Schedulers.elastic()),
          40)
          .map(v -> DefaultPayload.create(String.valueOf(counter.getAndIncrement())))
          .takeUntilOther(Flux.from(payloads).then());
    }

    private <T> Flux<T> duplicate(Flux<T> f, int n) {
      Flux<T> r = Flux.empty();
      for (int i = 0; i < n; i++) {
        r = r.mergeWith(f);
      }
      return r;
    }
  }
}
