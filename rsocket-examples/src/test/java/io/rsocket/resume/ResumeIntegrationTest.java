/*
 * Copyright 2015-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.resume;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.exceptions.RejectedResumeException;
import io.rsocket.exceptions.UnsupportedSetupException;
import io.rsocket.test.SlowTest;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

@SlowTest
public class ResumeIntegrationTest {
  private static final String SERVER_HOST = "localhost";
  private static final int SERVER_PORT = 0;

  @Test
  void timeoutOnPermanentDisconnect() {
    CloseableChannel closeable = newServerRSocket().block();

    DisconnectableClientTransport clientTransport =
        new DisconnectableClientTransport(clientTransport(closeable.address()));

    int sessionDurationSeconds = 5;
    RSocket rSocket = newClientRSocket(clientTransport, sessionDurationSeconds).block();

    Mono.delay(Duration.ofSeconds(1)).subscribe(v -> clientTransport.disconnectPermanently());

    StepVerifier.create(
            rSocket.requestChannel(testRequest()).then().doFinally(s -> closeable.dispose()))
        .expectError(ClosedChannelException.class)
        .verify(Duration.ofSeconds(7));
  }

  @Test
  public void reconnectOnDisconnect() {
    CloseableChannel closeable = newServerRSocket().block();

    DisconnectableClientTransport clientTransport =
        new DisconnectableClientTransport(clientTransport(closeable.address()));

    int sessionDurationSeconds = 15;
    RSocket rSocket = newClientRSocket(clientTransport, sessionDurationSeconds).block();

    Flux.just(3, 20, 40, 75)
        .flatMap(v -> Mono.delay(Duration.ofSeconds(v)))
        .subscribe(v -> clientTransport.disconnectFor(Duration.ofSeconds(7)));

    AtomicInteger counter = new AtomicInteger(-1);
    StepVerifier.create(
            rSocket
                .requestChannel(testRequest())
                .take(Duration.ofSeconds(600))
                .map(Payload::getDataUtf8)
                .timeout(Duration.ofSeconds(12))
                .doOnNext(x -> throwOnNonContinuous(counter, x))
                .then()
                .doFinally(s -> closeable.dispose()))
        .expectComplete()
        .verify();
  }

  @Test
  public void reconnectOnMissingSession() {

    int serverSessionDuration = 2;

    CloseableChannel closeable = newServerRSocket(serverSessionDuration).block();

    DisconnectableClientTransport clientTransport =
        new DisconnectableClientTransport(clientTransport(closeable.address()));
    ErrorConsumer errorConsumer = new ErrorConsumer();
    int clientSessionDurationSeconds = 10;

    RSocket rSocket =
        newClientRSocket(clientTransport, clientSessionDurationSeconds, errorConsumer).block();

    Mono.delay(Duration.ofSeconds(1))
        .subscribe(v -> clientTransport.disconnectFor(Duration.ofSeconds(3)));

    StepVerifier.create(
            rSocket.requestChannel(testRequest()).then().doFinally(s -> closeable.dispose()))
        .expectError()
        .verify(Duration.ofSeconds(5));

    StepVerifier.create(errorConsumer.errors().next())
        .expectNextMatches(
            err ->
                err instanceof RejectedResumeException
                    && "unknown resume token".equals(err.getMessage()))
        .expectComplete()
        .verify(Duration.ofSeconds(5));
  }

  @Test
  void serverMissingResume() {
    CloseableChannel closeableChannel =
        RSocketFactory.receive()
            .acceptor((setupPayload, rSocket) -> Mono.just(new TestResponderRSocket()))
            .transport(serverTransport(SERVER_HOST, SERVER_PORT))
            .start()
            .block();

    ErrorConsumer errorConsumer = new ErrorConsumer();

    RSocket rSocket =
        RSocketFactory.connect()
            .resume()
            .errorConsumer(errorConsumer)
            .transport(clientTransport(closeableChannel.address()))
            .start()
            .block();

    StepVerifier.create(errorConsumer.errors().next().doFinally(s -> closeableChannel.dispose()))
        .expectNextMatches(
            err ->
                err instanceof UnsupportedSetupException
                    && "resume not supported".equals(err.getMessage()))
        .expectComplete()
        .verify(Duration.ofSeconds(5));

    StepVerifier.create(rSocket.onClose()).expectComplete().verify(Duration.ofSeconds(5));
    Assertions.assertThat(rSocket.isDisposed()).isTrue();
  }

  static ClientTransport clientTransport(InetSocketAddress address) {
    return TcpClientTransport.create(address);
  }

  static ServerTransport<CloseableChannel> serverTransport(String host, int port) {
    return TcpServerTransport.create(host, port);
  }

  private static class ErrorConsumer implements Consumer<Throwable> {
    private final ReplayProcessor<Throwable> errors = ReplayProcessor.create();

    public Flux<Throwable> errors() {
      return errors;
    }

    @Override
    public void accept(Throwable throwable) {
      errors.onNext(throwable);
    }
  }

  private static Flux<Payload> testRequest() {
    return Flux.interval(Duration.ofMillis(50))
        .map(v -> DefaultPayload.create("client_request"))
        .onBackpressureDrop();
  }

  private void throwOnNonContinuous(AtomicInteger counter, String x) {
    int curValue = Integer.parseInt(x);
    int prevValue = counter.get();
    if (prevValue >= 0) {
      int dif = curValue - prevValue;
      if (dif != 1) {
        throw new IllegalStateException(
            String.format(
                "Payload values are expected to be continuous numbers: %d %d",
                prevValue, curValue));
      }
    }
    counter.set(curValue);
  }

  private static Mono<RSocket> newClientRSocket(
      DisconnectableClientTransport clientTransport, int sessionDurationSeconds) {
    return newClientRSocket(clientTransport, sessionDurationSeconds, err -> {});
  }

  private static Mono<RSocket> newClientRSocket(
      DisconnectableClientTransport clientTransport,
      int sessionDurationSeconds,
      Consumer<Throwable> errConsumer) {
    return RSocketFactory.connect()
        .resume()
        .resumeSessionDuration(Duration.ofSeconds(sessionDurationSeconds))
        .resumeStore(t -> new InMemoryResumableFramesStore("client", 500_000))
        .resumeCleanupOnKeepAlive()
        .keepAliveTickPeriod(Duration.ofSeconds(5))
        .keepAliveAckTimeout(Duration.ofMinutes(5))
        .errorConsumer(errConsumer)
        .resumeStrategy(() -> new PeriodicResumeStrategy(Duration.ofSeconds(1)))
        .transport(clientTransport)
        .start();
  }

  private static Mono<CloseableChannel> newServerRSocket() {
    return newServerRSocket(15);
  }

  private static Mono<CloseableChannel> newServerRSocket(int sessionDurationSeconds) {
    return RSocketFactory.receive()
        .resume()
        .resumeStore(t -> new InMemoryResumableFramesStore("server", 500_000))
        .resumeSessionDuration(Duration.ofSeconds(sessionDurationSeconds))
        .resumeCleanupOnKeepAlive()
        .acceptor((setupPayload, rSocket) -> Mono.just(new TestResponderRSocket()))
        .transport(serverTransport(SERVER_HOST, SERVER_PORT))
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
              20)
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
