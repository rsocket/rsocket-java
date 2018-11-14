/*
 * Copyright 2015-2018 the original author or authors.
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

package io.rsocket.test;

import io.rsocket.Closeable;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.util.DefaultPayload;
import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public interface TransportTest {

  @AfterEach
  default void close() {
    getTransportPair().dispose();
  }

  default Payload createTestPayload(int metadataPresent) {
    String metadata1;

    switch (metadataPresent % 5) {
      case 0:
        metadata1 = null;
        break;
      case 1:
        metadata1 = "";
        break;
      default:
        metadata1 = "metadata";
        break;
    }
    String metadata = metadata1;

    return DefaultPayload.create("test-data", metadata);
  }

  @DisplayName("makes 10 fireAndForget requests")
  @Test
  default void fireAndForget10() {
    Flux.range(1, 10)
        .flatMap(i -> getClient().fireAndForget(createTestPayload(i)))
        .as(StepVerifier::create)
        .expectNextCount(0)
        .expectComplete()
        .verify(getTimeout());
  }

  default RSocket getClient() {
    return getTransportPair().getClient();
  }

  Duration getTimeout();

  TransportPair getTransportPair();

  @DisplayName("makes 10 metadataPush requests")
  @Test
  default void metadataPush10() {
    Flux.range(1, 10)
        .flatMap(i -> getClient().metadataPush(DefaultPayload.create("", "test-metadata")))
        .as(StepVerifier::create)
        .expectNextCount(0)
        .expectComplete()
        .verify(getTimeout());
  }

  @DisplayName("makes 1 requestChannel request with 0 payloads")
  @Test
  default void requestChannel0() {
    getClient()
        .requestChannel(Flux.empty())
        .as(StepVerifier::create)
        .expectNextCount(0)
        .expectComplete()
        .verify(getTimeout());
  }

  @DisplayName("makes 1 requestChannel request with 1 payloads")
  @Test
  default void requestChannel1() {
    getClient()
        .requestChannel(Mono.just(createTestPayload(0)))
        .as(StepVerifier::create)
        .expectNextCount(1)
        .expectComplete()
        .verify(getTimeout());
  }

  @DisplayName("makes 1 requestChannel request with 200,000 payloads")
  @Test
  default void requestChannel200_000() {
    Flux<Payload> payloads = Flux.range(0, 200_000).map(this::createTestPayload);

    getClient()
        .requestChannel(payloads)
        .as(StepVerifier::create)
        .expectNextCount(200_000)
        .expectComplete()
        .verify(getTimeout());
  }

  @DisplayName("makes 1 requestChannel request with 20,000 payloads")
  @Test
  default void requestChannel20_000() {
    Flux<Payload> payloads = Flux.range(0, 20_000).map(this::createTestPayload);

    getClient()
        .requestChannel(payloads)
        .as(StepVerifier::create)
        .expectNextCount(20_000)
        .expectComplete()
        .verify(getTimeout());
  }

  @DisplayName("makes 1 requestChannel request with 2,000,000 payloads")
  @SlowTest
  default void requestChannel2_000_000() {
    Flux<Payload> payloads = Flux.range(0, 2_000_000).map(this::createTestPayload);

    getClient()
        .requestChannel(payloads)
        .as(StepVerifier::create)
        .expectNextCount(2_000_000)
        .expectComplete()
        .verify(getTimeout());
  }

  @DisplayName("makes 1 requestChannel request with 3 payloads")
  @Test
  default void requestChannel3() {
    Flux<Payload> payloads = Flux.range(0, 3).map(this::createTestPayload);

    getClient()
        .requestChannel(payloads)
        .as(StepVerifier::create)
        .expectNextCount(3)
        .expectComplete()
        .verify(getTimeout());
  }

  @DisplayName("makes 1 requestChannel request with 512 payloads")
  @Test
  default void requestChannel512() {
    Flux<Payload> payloads = Flux.range(0, 512).map(this::createTestPayload);

    getClient()
        .requestChannel(payloads)
        .log()
        .as(StepVerifier::create)
        .expectNextCount(512)
        .as("expected 512 items")
        .expectComplete()
        .verify(getTimeout());
  }

  @DisplayName("makes 1 requestResponse request")
  @Test
  default void requestResponse1() {
    getClient()
        .requestResponse(createTestPayload(1))
        .map(Payload::getDataUtf8)
        .as(StepVerifier::create)
        .expectNextCount(1)
        .expectComplete()
        .verify(getTimeout());
  }

  @DisplayName("makes 10 requestResponse requests")
  @Test
  default void requestResponse10() {
    Flux.range(1, 10)
        .flatMap(i -> getClient().requestResponse(createTestPayload(i)).map(Payload::getDataUtf8))
        .as(StepVerifier::create)
        .expectNextCount(10)
        .expectComplete()
        .verify(getTimeout());
  }

  @DisplayName("makes 100 requestResponse requests")
  @Test
  default void requestResponse100() {
    Flux.range(1, 100)
        .flatMap(i -> getClient().requestResponse(createTestPayload(i)).map(Payload::getDataUtf8))
        .as(StepVerifier::create)
        .expectNextCount(100)
        .expectComplete()
        .verify(getTimeout());
  }

  @DisplayName("makes 10,000 requestResponse requests")
  @Test
  default void requestResponse10_000() {
    Flux.range(1, 10_000)
        .flatMap(i -> getClient().requestResponse(createTestPayload(i)).map(Payload::getDataUtf8))
        .as(StepVerifier::create)
        .expectNextCount(10_000)
        .expectComplete()
        .verify(getTimeout());
  }

  @DisplayName("makes 1 requestStream request and receives 10,000 responses")
  @Test
  default void requestStream10_000() {
    getClient()
        .requestStream(createTestPayload(3))
        .as(StepVerifier::create)
        .expectNextCount(10_000)
        .expectComplete()
        .verify(getTimeout());
  }

  @DisplayName("makes 1 requestStream request and receives 5 responses")
  @Test
  default void requestStream5() {
    getClient()
        .requestStream(createTestPayload(3))
        .take(5)
        .as(StepVerifier::create)
        .expectNextCount(5)
        .expectComplete()
        .verify(getTimeout());
  }

  @DisplayName("makes 1 requestStream request and consumes result incrementally")
  @Test
  default void requestStreamDelayedRequestN() {
    getClient()
        .requestStream(createTestPayload(3))
        .take(10)
        .as(StepVerifier::create)
        .thenRequest(5)
        .expectNextCount(5)
        .thenRequest(5)
        .expectNextCount(5)
        .expectComplete()
        .verify(getTimeout());
  }

  final class TransportPair<T, S extends Closeable> implements Disposable {

    private final RSocket client;

    private final S server;

    public TransportPair(
        Supplier<T> addressSupplier,
        BiFunction<T, S, ClientTransport> clientTransportSupplier,
        Function<T, ServerTransport<S>> serverTransportSupplier) {

      T address = addressSupplier.get();

      server =
          RSocketFactory.receive()
              .acceptor((setup, sendingSocket) -> Mono.just(new TestRSocket()))
              .transport(serverTransportSupplier.apply(address))
              .start()
              .block();

      client =
          RSocketFactory.connect()
              .transport(clientTransportSupplier.apply(address, server))
              .start()
              .doOnError(Throwable::printStackTrace)
              .block();
    }

    @Override
    public void dispose() {
      server.dispose();
    }

    RSocket getClient() {
      return client;
    }
  }
}
