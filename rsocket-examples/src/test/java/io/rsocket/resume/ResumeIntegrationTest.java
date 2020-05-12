/*
 * Copyright 2015-2020 the original author or authors.
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

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.core.Resume;
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
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.retry.Retry;

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
    int clientSessionDurationSeconds = 10;

    RSocket rSocket = newClientRSocket(clientTransport, clientSessionDurationSeconds).block();

    Mono.delay(Duration.ofSeconds(1))
        .subscribe(v -> clientTransport.disconnectFor(Duration.ofSeconds(3)));

    StepVerifier.create(
            rSocket.requestChannel(testRequest()).then().doFinally(s -> closeable.dispose()))
        .expectError()
        .verify(Duration.ofSeconds(5));

    StepVerifier.create(rSocket.onClose())
        .expectErrorMatches(
            err ->
                err instanceof RejectedResumeException
                    && "unknown resume token".equals(err.getMessage()))
        .verify(Duration.ofSeconds(5));
  }

  @Test
  void serverMissingResume() {
    CloseableChannel closeableChannel =
        RSocketServer.create(SocketAcceptor.with(new TestResponderRSocket()))
            .bind(serverTransport(SERVER_HOST, SERVER_PORT))
            .block();

    RSocket rSocket =
        RSocketConnector.create()
            .resume(new Resume())
            .connect(clientTransport(closeableChannel.address()))
            .block();

    StepVerifier.create(rSocket.onClose().doFinally(s -> closeableChannel.dispose()))
        .expectErrorMatches(
            err ->
                err instanceof UnsupportedSetupException
                    && "resume not supported".equals(err.getMessage()))
        .verify(Duration.ofSeconds(5));

    Assertions.assertThat(rSocket.isDisposed()).isTrue();
  }

  static ClientTransport clientTransport(InetSocketAddress address) {
    return TcpClientTransport.create(address);
  }

  static ServerTransport<CloseableChannel> serverTransport(String host, int port) {
    return TcpServerTransport.create(host, port);
  }

  private static Flux<Payload> testRequest() {
    return Flux.interval(Duration.ofMillis(500))
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
    return RSocketConnector.create()
        .resume(
            new Resume()
                .sessionDuration(Duration.ofSeconds(sessionDurationSeconds))
                .storeFactory(t -> new InMemoryResumableFramesStore("client", 500_000))
                .cleanupStoreOnKeepAlive()
                .retry(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(1))))
        .keepAlive(Duration.ofSeconds(5), Duration.ofMinutes(5))
        .connect(clientTransport);
  }

  private static Mono<CloseableChannel> newServerRSocket() {
    return newServerRSocket(15);
  }

  private static Mono<CloseableChannel> newServerRSocket(int sessionDurationSeconds) {
    return RSocketServer.create(SocketAcceptor.with(new TestResponderRSocket()))
        .resume(
            new Resume()
                .sessionDuration(Duration.ofSeconds(sessionDurationSeconds))
                .cleanupStoreOnKeepAlive()
                .storeFactory(t -> new InMemoryResumableFramesStore("server", 500_000)))
        .bind(serverTransport(SERVER_HOST, SERVER_PORT));
  }

  private static class TestResponderRSocket implements RSocket {

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
