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
import io.rsocket.exceptions.RejectedSetupException;
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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
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

public class ResumeIntegrationTest {
  private static final String SERVER_HOST = "localhost";
  private static final int SERVER_PORT = 0;

  @Test
  void timeoutOnPermanentDisconnect() {
    int sessionDurationSeconds = 5;
    CloseableChannel server = newResumableServer().block();
    DisconnectableClientTransport clientTransport =
        new DisconnectableClientTransport(clientTransport(server.address()));

    RSocket client = newResumableClient(clientTransport, sessionDurationSeconds).block();

    Mono.delay(Duration.ofSeconds(1)).subscribe(v -> clientTransport.disconnectPermanently());

    StepVerifier.create(
            client.requestChannel(testRequest()).then().doFinally(s -> server.dispose()))
        .expectError(ClosedChannelException.class)
        .verify(Duration.ofSeconds(7));
  }

  @SlowTest
  public void reconnectOnDisconnect() {
    int sessionDurationSeconds = 15;
    CloseableChannel server = newResumableServer().block();
    DisconnectableClientTransport clientTransport =
        new DisconnectableClientTransport(clientTransport(server.address()));

    RSocket client = newResumableClient(clientTransport, sessionDurationSeconds).block();
    Flux.just(3, 20, 40, 75)
        .flatMap(v -> Mono.delay(Duration.ofSeconds(v)))
        .subscribe(v -> clientTransport.disconnectFor(Duration.ofSeconds(7)));

    AtomicInteger counter = new AtomicInteger(-1);
    StepVerifier.create(
            client
                .requestChannel(testRequest())
                .take(Duration.ofSeconds(600))
                .map(Payload::getDataUtf8)
                .timeout(Duration.ofSeconds(12))
                .doOnNext(x -> throwOnNonContinuous(counter, x))
                .then()
                .doFinally(s -> server.dispose()))
        .expectComplete()
        .verify();
  }

  @Test
  public void reconnectOnMissingSession() {
    int serverSessionDurationSeconds = 2;
    int clientSessionDurationSeconds = 10;
    CloseableChannel server = newResumableServer(serverSessionDurationSeconds).block();
    DisconnectableClientTransport clientTransport =
        new DisconnectableClientTransport(clientTransport(server.address()));
    ErrorConsumer errorConsumer = new ErrorConsumer();

    RSocket client =
        newResumableClient(clientTransport, clientSessionDurationSeconds, errorConsumer).block();

    Mono.delay(Duration.ofSeconds(1))
        .subscribe(v -> clientTransport.disconnectFor(Duration.ofSeconds(3)));

    StepVerifier.create(
            client.requestChannel(testRequest()).then().doFinally(s -> server.dispose()))
        .expectError()
        .verify(Duration.ofSeconds(5));

    StepVerifier.create(errorConsumer.errors().next())
        .expectNextMatches(
            err ->
                err instanceof RejectedResumeException
                    && "unknown resume token".equals(err.getMessage()))
        .expectComplete()
        .verify(Duration.ofSeconds(5));

    StepVerifier.create(client.onClose()).expectComplete().verify(Duration.ofSeconds(5));
    Assertions.assertThat(client.isDisposed()).isTrue();
  }

  @Test
  void serverMissingResume() {
    int sessionDurationSeconds = 15;
    CloseableChannel nonResumableServer = newServer().block();
    ErrorConsumer errorConsumer = new ErrorConsumer();

    RSocket resumableClient =
        newResumableClient(
                clientTransport(nonResumableServer.address()),
                sessionDurationSeconds,
                errorConsumer)
            .block();

    StepVerifier.create(errorConsumer.errors().next().doFinally(s -> nonResumableServer.dispose()))
        .expectNextMatches(
            err ->
                err instanceof UnsupportedSetupException
                    && "resume not supported".equals(err.getMessage()))
        .expectComplete()
        .verify(Duration.ofSeconds(5));

    StepVerifier.create(resumableClient.onClose()).expectComplete().verify(Duration.ofSeconds(5));
    Assertions.assertThat(resumableClient.isDisposed()).isTrue();
  }

  @Test
  void duplicateSession() {
    int sessionDurationSeconds = 15;
    CloseableChannel server = newResumableServer(sessionDurationSeconds).block();
    ErrorConsumer errorConsumer = new ErrorConsumer();
    ResumeToken token = ResumeToken.fromBytes("testToken".getBytes(StandardCharsets.UTF_8));

    Mono<RSocket> client =
        newResumableClient(
            clientTransport(server.address()),
            sessionDurationSeconds,
            errorConsumer,
            Optional.of(token));

    Flux.just(0, 500)
        .flatMap(delay -> Mono.delay(Duration.ofMillis(delay)))
        .flatMap(v -> client)
        .subscribe();

    StepVerifier.create(errorConsumer.errors().next().doFinally(s -> server.dispose()))
        .expectNextMatches(
            err ->
                err instanceof RejectedSetupException
                    && "duplicate session".equals(err.getMessage()))
        .expectComplete()
        .verify(Duration.ofSeconds(5));
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

  private static Mono<RSocket> newResumableClient(
      ClientTransport clientTransport, int sessionDurationSeconds) {
    return newResumableClient(clientTransport, sessionDurationSeconds, err -> {});
  }

  private static Mono<RSocket> newResumableClient(
      ClientTransport clientTransport,
      int sessionDurationSeconds,
      Consumer<Throwable> errConsumer) {
    return newResumableClient(
        clientTransport, sessionDurationSeconds, errConsumer, Optional.empty());
  }

  private static Mono<RSocket> newResumableClient(
      ClientTransport clientTransport,
      int sessionDurationSeconds,
      Consumer<Throwable> errConsumer,
      Optional<ResumeToken> resumeToken) {

    RSocketFactory.ClientRSocketFactory clientRSocketFactory =
        RSocketFactory.connect()
            .resume()
            .resumeSessionDuration(Duration.ofSeconds(sessionDurationSeconds))
            .keepAliveTickPeriod(Duration.ofSeconds(30))
            .keepAliveAckTimeout(Duration.ofMinutes(5))
            .errorConsumer(errConsumer)
            .resumeStrategy(() -> new PeriodicResumeStrategy(Duration.ofSeconds(1)));
    if (resumeToken.isPresent()) {
      clientRSocketFactory.resumeToken(resumeToken::get);
    }
    return clientRSocketFactory.transport(clientTransport).start();
  }

  private static Mono<CloseableChannel> newResumableServer() {
    return newResumableServer(15);
  }

  private static Mono<CloseableChannel> newResumableServer(int sessionDurationSeconds) {
    return RSocketFactory.receive()
        .resume()
        .resumeStore(t -> new InMemoryResumableFramesStore("server", 100_000))
        .resumeSessionDuration(Duration.ofSeconds(sessionDurationSeconds))
        .acceptor((setupPayload, rSocket) -> Mono.just(new TestResponderRSocket()))
        .transport(serverTransport(SERVER_HOST, SERVER_PORT))
        .start();
  }

  private Mono<CloseableChannel> newServer() {
    return RSocketFactory.receive()
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
