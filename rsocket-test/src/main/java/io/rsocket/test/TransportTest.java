/*
 * Copyright 2015-2023 the original author or authors.
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.netty.util.ResourceLeakDetector;
import io.rsocket.Closeable;
import io.rsocket.DuplexConnection;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketErrorException;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.core.Resume;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.plugins.DuplexConnectionInterceptor;
import io.rsocket.resume.InMemoryResumableFramesStore;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.util.ByteBufPayload;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.Logger;
import reactor.util.Loggers;

public interface TransportTest {

  Logger logger = Loggers.getLogger(TransportTest.class);

  String MOCK_DATA = "test-data";
  String MOCK_METADATA = "metadata";
  String LARGE_DATA = read("words.shakespeare.txt.gz");
  Payload LARGE_PAYLOAD = ByteBufPayload.create(LARGE_DATA, LARGE_DATA);

  static String read(String resourceName) {
    try (BufferedReader br =
        new BufferedReader(
            new InputStreamReader(
                new GZIPInputStream(
                    TransportTest.class.getClassLoader().getResourceAsStream(resourceName))))) {

      return br.lines().map(String::toLowerCase).collect(Collectors.joining("\n\r"));
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeEach
  default void setup() {
    Hooks.onOperatorDebug();
  }

  @AfterEach
  default void close() {
    try {
      logger.debug("------------------Awaiting communication to finish------------------");
      getTransportPair().responder.awaitAllInteractionTermination(getTimeout());
      logger.debug("---------------------Disposing Client And Server--------------------");
      getTransportPair().dispose();
      getTransportPair().awaitClosed(getTimeout());
      logger.debug("------------------------Disposing Schedulers-------------------------");
      Schedulers.parallel().disposeGracefully().timeout(getTimeout(), Mono.empty()).block();
      Schedulers.boundedElastic().disposeGracefully().timeout(getTimeout(), Mono.empty()).block();
      Schedulers.single().disposeGracefully().timeout(getTimeout(), Mono.empty()).block();
      logger.debug("---------------------------Leaks Checking----------------------------");
      RuntimeException throwable =
          new RuntimeException() {
            @Override
            public synchronized Throwable fillInStackTrace() {
              return this;
            }

            @Override
            public String getMessage() {
              return Arrays.toString(getSuppressed());
            }
          };

      try {
        getTransportPair().byteBufAllocator2.assertHasNoLeaks();
      } catch (Throwable t) {
        throwable = Exceptions.addSuppressed(throwable, t);
      }

      try {
        getTransportPair().byteBufAllocator1.assertHasNoLeaks();
      } catch (Throwable t) {
        throwable = Exceptions.addSuppressed(throwable, t);
      }

      if (throwable.getSuppressed().length > 0) {
        throw throwable;
      }
    } finally {
      Hooks.resetOnOperatorDebug();
      Schedulers.resetOnHandleError();
    }
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
        metadata1 = MOCK_METADATA;
        break;
    }
    String metadata = metadata1;

    return ByteBufPayload.create(MOCK_DATA, metadata);
  }

  @DisplayName("makes 10 fireAndForget requests")
  @Test
  default void fireAndForget10() {
    Flux.range(1, 10)
        .flatMap(i -> getClient().fireAndForget(createTestPayload(i)))
        .as(StepVerifier::create)
        .expectComplete()
        .verify(getTimeout());

    getTransportPair().responder.awaitUntilObserved(10, getTimeout());
  }

  @DisplayName("makes 10 fireAndForget with Large Payload in Requests")
  @Test
  default void largePayloadFireAndForget10() {
    Flux.range(1, 10)
        .flatMap(i -> getClient().fireAndForget(LARGE_PAYLOAD.retain()))
        .as(StepVerifier::create)
        .expectComplete()
        .verify(getTimeout());

    getTransportPair().responder.awaitUntilObserved(10, getTimeout());
  }

  default RSocket getClient() {
    return getTransportPair().getClient();
  }

  Duration getTimeout();

  TransportPair getTransportPair();

  @DisplayName("makes 10 metadataPush requests")
  @Test
  default void metadataPush10() {
    Assumptions.assumeThat(getTransportPair().withResumability).isFalse();
    Flux.range(1, 10)
        .flatMap(i -> getClient().metadataPush(ByteBufPayload.create("", "test-metadata")))
        .as(StepVerifier::create)
        .expectComplete()
        .verify(getTimeout());

    getTransportPair().responder.awaitUntilObserved(10, getTimeout());
  }

  @DisplayName("makes 10 metadataPush with Large Metadata in requests")
  @Test
  default void largePayloadMetadataPush10() {
    Assumptions.assumeThat(getTransportPair().withResumability).isFalse();
    Flux.range(1, 10)
        .flatMap(i -> getClient().metadataPush(ByteBufPayload.create("", LARGE_DATA)))
        .as(StepVerifier::create)
        .expectComplete()
        .verify(getTimeout());

    getTransportPair().responder.awaitUntilObserved(10, getTimeout());
  }

  @DisplayName("makes 1 requestChannel request with 0 payloads")
  @Test
  default void requestChannel0() {
    getClient()
        .requestChannel(Flux.empty())
        .as(StepVerifier::create)
        .expectErrorSatisfies(
            t ->
                Assertions.assertThat(t)
                    .isInstanceOf(CancellationException.class)
                    .hasMessage("Empty Source"))
        .verify(getTimeout());
  }

  @DisplayName("makes 1 requestChannel request with 1 payloads")
  @Test
  default void requestChannel1() {
    getClient()
        .requestChannel(Mono.just(createTestPayload(0)))
        .doOnNext(Payload::release)
        .as(StepVerifier::create)
        .thenConsumeWhile(new PayloadPredicate(1))
        .expectComplete()
        .verify(getTimeout());
  }

  @DisplayName("makes 1 requestChannel request with 200,000 payloads")
  @Test
  default void requestChannel200_000() {
    Flux<Payload> payloads = Flux.range(0, 200_000).map(this::createTestPayload);

    getClient()
        .requestChannel(payloads)
        .doOnNext(Payload::release)
        .limitRate(8)
        .as(StepVerifier::create)
        .thenConsumeWhile(new PayloadPredicate(200_000))
        .expectComplete()
        .verify(getTimeout());
  }

  @DisplayName("makes 1 requestChannel request with 50 large payloads")
  @Test
  default void largePayloadRequestChannel50() {
    Flux<Payload> payloads = Flux.range(0, 50).map(__ -> LARGE_PAYLOAD.retain());

    getClient()
        .requestChannel(payloads)
        .doOnNext(Payload::release)
        .as(StepVerifier::create)
        .thenConsumeWhile(new PayloadPredicate(50))
        .expectComplete()
        .verify(getTimeout());
  }

  @DisplayName("makes 1 requestChannel request with 20,000 payloads")
  @Test
  default void requestChannel20_000() {
    Flux<Payload> payloads = Flux.range(0, 20_000).map(metadataPresent -> createTestPayload(7));

    getClient()
        .requestChannel(payloads)
        .doOnNext(this::assertChannelPayload)
        .doOnNext(Payload::release)
        .as(StepVerifier::create)
        .thenConsumeWhile(new PayloadPredicate(20_000))
        .expectComplete()
        .verify(getTimeout());
  }

  @DisplayName("makes 1 requestChannel request with 2,000,000 payloads")
  @SlowTest
  default void requestChannel2_000_000() {
    Flux<Payload> payloads = Flux.range(0, 2_000_000).map(this::createTestPayload);

    getClient()
        .requestChannel(payloads)
        .doOnNext(Payload::release)
        .limitRate(8)
        .as(StepVerifier::create)
        .thenConsumeWhile(new PayloadPredicate(2_000_000))
        .expectComplete()
        .verify(getTimeout());
  }

  @DisplayName("makes 1 requestChannel request with 3 payloads")
  @Test
  default void requestChannel3() {
    AtomicLong requested = new AtomicLong();
    Flux<Payload> payloads =
        Flux.range(0, 3).doOnRequest(requested::addAndGet).map(this::createTestPayload);

    getClient()
        .requestChannel(payloads)
        .doOnNext(Payload::release)
        .as(publisher -> StepVerifier.create(publisher, 3))
        .thenConsumeWhile(new PayloadPredicate(3))
        .expectComplete()
        .verify(getTimeout());

    Assertions.assertThat(requested.get()).isEqualTo(3L);
  }

  @DisplayName("makes 1 requestChannel request with 256 payloads")
  @Test
  default void requestChannel256() {
    AtomicInteger counter = new AtomicInteger();
    Flux<Payload> payloads =
        Flux.defer(
            () -> {
              final int subscription = counter.getAndIncrement();
              return Flux.range(0, 256)
                  .map(i -> "S{" + subscription + "}: Data{" + i + "}")
                  .map(data -> ByteBufPayload.create(data));
            });
    final Scheduler scheduler = Schedulers.fromExecutorService(Executors.newFixedThreadPool(12));

    try {
      Flux.range(0, 1024)
          .flatMap(v -> Mono.fromRunnable(() -> check(payloads)).subscribeOn(scheduler), 12)
          .blockLast();
    } finally {
      scheduler.disposeGracefully().block();
    }
  }

  default void check(Flux<Payload> payloads) {
    getClient()
        .requestChannel(payloads)
        .doOnNext(ReferenceCounted::release)
        .limitRate(8)
        .as(StepVerifier::create)
        .thenConsumeWhile(new PayloadPredicate(256))
        .as("expected 256 items")
        .expectComplete()
        .verify(getTimeout());
  }

  @DisplayName("makes 1 requestResponse request")
  @Test
  default void requestResponse1() {
    getClient()
        .requestResponse(createTestPayload(1))
        .doOnNext(this::assertPayload)
        .doOnNext(Payload::release)
        .as(StepVerifier::create)
        .expectNextCount(1)
        .expectComplete()
        .verify(getTimeout());
  }

  @DisplayName("makes 10 requestResponse requests")
  @Test
  default void requestResponse10() {
    Flux.range(1, 10)
        .flatMap(
            i ->
                getClient()
                    .requestResponse(createTestPayload(i))
                    .doOnNext(v -> assertPayload(v))
                    .doOnNext(Payload::release))
        .as(StepVerifier::create)
        .expectNextCount(10)
        .expectComplete()
        .verify(getTimeout());
  }

  @DisplayName("makes 100 requestResponse requests")
  @Test
  default void requestResponse100() {
    Flux.range(1, 100)
        .flatMap(i -> getClient().requestResponse(createTestPayload(i)).doOnNext(Payload::release))
        .as(StepVerifier::create)
        .expectNextCount(100)
        .expectComplete()
        .verify(getTimeout());
  }

  @DisplayName("makes 50 requestResponse requests")
  @Test
  default void largePayloadRequestResponse50() {
    Flux.range(1, 50)
        .flatMap(
            i -> getClient().requestResponse(LARGE_PAYLOAD.retain()).doOnNext(Payload::release))
        .as(StepVerifier::create)
        .expectNextCount(50)
        .expectComplete()
        .verify(getTimeout());
  }

  @DisplayName("makes 10,000 requestResponse requests")
  @Test
  default void requestResponse10_000() {
    Flux.range(1, 10_000)
        .flatMap(i -> getClient().requestResponse(createTestPayload(i)).doOnNext(Payload::release))
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
        .doOnNext(this::assertPayload)
        .doOnNext(Payload::release)
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
        .doOnNext(this::assertPayload)
        .doOnNext(Payload::release)
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
        .doOnNext(Payload::release)
        .as(StepVerifier::create)
        .thenRequest(5)
        .expectNextCount(5)
        .thenRequest(5)
        .expectNextCount(5)
        .expectComplete()
        .verify(getTimeout());
  }

  default void assertPayload(Payload p) {
    TransportPair transportPair = getTransportPair();
    if (!transportPair.expectedPayloadData().equals(p.getDataUtf8())
        || !transportPair.expectedPayloadMetadata().equals(p.getMetadataUtf8())) {
      throw new IllegalStateException("Unexpected payload");
    }
  }

  default void assertChannelPayload(Payload p) {
    if (!MOCK_DATA.equals(p.getDataUtf8()) || !MOCK_METADATA.equals(p.getMetadataUtf8())) {
      throw new IllegalStateException("Unexpected payload");
    }
  }

  class TransportPair<T, S extends Closeable> implements Disposable {

    private static final String data = "hello world";
    private static final String metadata = "metadata";

    private final boolean withResumability;
    private final boolean runClientWithAsyncInterceptors;
    private final boolean runServerWithAsyncInterceptors;

    private final LeaksTrackingByteBufAllocator byteBufAllocator1 =
        LeaksTrackingByteBufAllocator.instrument(
            ByteBufAllocator.DEFAULT, Duration.ofMinutes(1), "Client");
    private final LeaksTrackingByteBufAllocator byteBufAllocator2 =
        LeaksTrackingByteBufAllocator.instrument(
            ByteBufAllocator.DEFAULT, Duration.ofMinutes(1), "Server");

    private final TestRSocket responder;

    private final RSocket client;

    private final S server;

    public TransportPair(
        Supplier<T> addressSupplier,
        TriFunction<T, S, ByteBufAllocator, ClientTransport> clientTransportSupplier,
        BiFunction<T, ByteBufAllocator, ServerTransport<S>> serverTransportSupplier) {
      this(addressSupplier, clientTransportSupplier, serverTransportSupplier, false);
    }

    public TransportPair(
        Supplier<T> addressSupplier,
        TriFunction<T, S, ByteBufAllocator, ClientTransport> clientTransportSupplier,
        BiFunction<T, ByteBufAllocator, ServerTransport<S>> serverTransportSupplier,
        boolean withRandomFragmentation) {
      this(
          addressSupplier,
          clientTransportSupplier,
          serverTransportSupplier,
          withRandomFragmentation,
          false);
    }

    public TransportPair(
        Supplier<T> addressSupplier,
        TriFunction<T, S, ByteBufAllocator, ClientTransport> clientTransportSupplier,
        BiFunction<T, ByteBufAllocator, ServerTransport<S>> serverTransportSupplier,
        boolean withRandomFragmentation,
        boolean withResumability) {
      Schedulers.onHandleError((t, e) -> e.printStackTrace());
      Schedulers.resetFactory();

      this.withResumability = withResumability;

      T address = addressSupplier.get();

      this.runClientWithAsyncInterceptors = ThreadLocalRandom.current().nextBoolean();
      this.runServerWithAsyncInterceptors = ThreadLocalRandom.current().nextBoolean();

      ByteBufAllocator allocatorToSupply1;
      ByteBufAllocator allocatorToSupply2;
      if (ResourceLeakDetector.getLevel() == ResourceLeakDetector.Level.ADVANCED
          || ResourceLeakDetector.getLevel() == ResourceLeakDetector.Level.PARANOID) {
        logger.info("Using LeakTrackingByteBufAllocator");
        allocatorToSupply1 = byteBufAllocator1;
        allocatorToSupply2 = byteBufAllocator2;
      } else {
        allocatorToSupply1 = ByteBufAllocator.DEFAULT;
        allocatorToSupply2 = ByteBufAllocator.DEFAULT;
      }
      responder = new TestRSocket(TransportPair.data, metadata);
      final RSocketServer rSocketServer =
          RSocketServer.create((setup, sendingSocket) -> Mono.just(responder))
              .payloadDecoder(PayloadDecoder.ZERO_COPY)
              .interceptors(
                  registry -> {
                    if (runServerWithAsyncInterceptors && !withResumability) {
                      logger.info(
                          "Perform Integration Test with Async Interceptors Enabled For Server");
                      registry
                          .forConnection(
                              (type, duplexConnection) ->
                                  new AsyncDuplexConnection(duplexConnection, "server"))
                          .forSocketAcceptor(
                              delegate ->
                                  (connectionSetupPayload, sendingSocket) ->
                                      delegate
                                          .accept(connectionSetupPayload, sendingSocket)
                                          .subscribeOn(Schedulers.parallel()));
                    }

                    if (withResumability) {
                      registry.forConnection(
                          (type, duplexConnection) ->
                              type == DuplexConnectionInterceptor.Type.SOURCE
                                  ? new DisconnectingDuplexConnection(
                                      "Server",
                                      duplexConnection,
                                      Duration.ofMillis(
                                          ThreadLocalRandom.current().nextInt(100, 1000)))
                                  : duplexConnection);
                    }
                  });

      if (withResumability) {
        rSocketServer.resume(
            new Resume()
                .storeFactory(
                    token -> new InMemoryResumableFramesStore("server", token, Integer.MAX_VALUE)));
      }

      if (withRandomFragmentation) {
        rSocketServer.fragment(ThreadLocalRandom.current().nextInt(256, 512));
      }

      server =
          rSocketServer.bind(serverTransportSupplier.apply(address, allocatorToSupply2)).block();

      final RSocketConnector rSocketConnector =
          RSocketConnector.create()
              .payloadDecoder(PayloadDecoder.ZERO_COPY)
              .keepAlive(Duration.ofMillis(10), Duration.ofHours(1))
              .interceptors(
                  registry -> {
                    if (runClientWithAsyncInterceptors && !withResumability) {
                      logger.info(
                          "Perform Integration Test with Async Interceptors Enabled For Client");
                      registry
                          .forConnection(
                              (type, duplexConnection) ->
                                  new AsyncDuplexConnection(duplexConnection, "client"))
                          .forSocketAcceptor(
                              delegate ->
                                  (connectionSetupPayload, sendingSocket) ->
                                      delegate
                                          .accept(connectionSetupPayload, sendingSocket)
                                          .subscribeOn(Schedulers.parallel()));
                    }

                    if (withResumability) {
                      registry.forConnection(
                          (type, duplexConnection) ->
                              type == DuplexConnectionInterceptor.Type.SOURCE
                                  ? new DisconnectingDuplexConnection(
                                      "Client",
                                      duplexConnection,
                                      Duration.ofMillis(
                                          ThreadLocalRandom.current().nextInt(10, 1500)))
                                  : duplexConnection);
                    }
                  });

      if (withResumability) {
        rSocketConnector.resume(
            new Resume()
                .storeFactory(
                    token -> new InMemoryResumableFramesStore("client", token, Integer.MAX_VALUE)));
      }

      if (withRandomFragmentation) {
        rSocketConnector.fragment(ThreadLocalRandom.current().nextInt(256, 512));
      }

      client =
          rSocketConnector
              .connect(clientTransportSupplier.apply(address, server, allocatorToSupply1))
              .doOnError(Throwable::printStackTrace)
              .block();
    }

    @Override
    public void dispose() {
      logger.info("terminating transport pair");
      client.dispose();
    }

    RSocket getClient() {
      return client;
    }

    public String expectedPayloadData() {
      return data;
    }

    public String expectedPayloadMetadata() {
      return metadata;
    }

    public void awaitClosed(Duration timeout) {
      logger.info("awaiting termination of transport pair");
      logger.info(
          "wrappers combination: client{async="
              + runClientWithAsyncInterceptors
              + "; resume="
              + withResumability
              + "} server{async="
              + runServerWithAsyncInterceptors
              + "; resume="
              + withResumability
              + "}");
      client
          .onClose()
          .doOnSubscribe(s -> logger.info("Client termination stage=onSubscribe(" + s + ")"))
          .doOnEach(s -> logger.info("Client termination stage=" + s))
          .onErrorResume(t -> Mono.empty())
          .doOnTerminate(() -> logger.info("Client terminated. Terminating Server"))
          .then(Mono.fromRunnable(server::dispose))
          .then(
              server
                  .onClose()
                  .doOnSubscribe(
                      s -> logger.info("Server termination stage=onSubscribe(" + s + ")"))
                  .doOnEach(s -> logger.info("Server termination stage=" + s)))
          .onErrorResume(t -> Mono.empty())
          .block(timeout);

      logger.info("TransportPair has been terminated");
    }

    private static class AsyncDuplexConnection implements DuplexConnection {

      private final DuplexConnection duplexConnection;
      private String tag;
      private final ByteBufReleaserOperator bufReleaserOperator;

      public AsyncDuplexConnection(DuplexConnection duplexConnection, String tag) {
        this.duplexConnection = duplexConnection;
        this.tag = tag;
        this.bufReleaserOperator = new ByteBufReleaserOperator();
      }

      @Override
      public void sendFrame(int streamId, ByteBuf frame) {
        duplexConnection.sendFrame(streamId, frame);
      }

      @Override
      public void sendErrorAndClose(RSocketErrorException e) {
        duplexConnection.sendErrorAndClose(e);
      }

      @Override
      public Flux<ByteBuf> receive() {
        return duplexConnection
            .receive()
            .doOnTerminate(() -> logger.info("[" + this + "] Receive is done before PO"))
            .subscribeOn(Schedulers.boundedElastic())
            .doOnNext(ByteBuf::retain)
            .publishOn(Schedulers.boundedElastic(), Integer.MAX_VALUE)
            .doOnTerminate(() -> logger.info("[" + this + "] Receive is done after PO"))
            .doOnDiscard(ReferenceCounted.class, ReferenceCountUtil::safeRelease)
            .transform(
                Operators.<ByteBuf, ByteBuf>lift(
                    (__, actual) -> {
                      bufReleaserOperator.actual = actual;
                      return bufReleaserOperator;
                    }));
      }

      @Override
      public ByteBufAllocator alloc() {
        return duplexConnection.alloc();
      }

      @Override
      public SocketAddress remoteAddress() {
        return duplexConnection.remoteAddress();
      }

      @Override
      public Mono<Void> onClose() {
        return Mono.whenDelayError(
            duplexConnection
                .onClose()
                .doOnTerminate(() -> logger.info("[" + this + "] Source Connection is done")),
            bufReleaserOperator
                .onClose()
                .doOnTerminate(() -> logger.info("[" + this + "] BufferReleaser is done")));
      }

      @Override
      public void dispose() {
        duplexConnection.dispose();
      }

      @Override
      public String toString() {
        return "AsyncDuplexConnection{"
            + "duplexConnection="
            + duplexConnection
            + ", tag='"
            + tag
            + '\''
            + ", bufReleaserOperator="
            + bufReleaserOperator
            + '}';
      }
    }

    private static class DisconnectingDuplexConnection implements DuplexConnection {

      private final String tag;
      final DuplexConnection source;
      final Duration delay;
      final Disposable.Swap disposables = Disposables.swap();

      DisconnectingDuplexConnection(String tag, DuplexConnection source, Duration delay) {
        this.tag = tag;
        this.source = source;
        this.delay = delay;
      }

      @Override
      public void dispose() {
        disposables.dispose();
        source.dispose();
      }

      @Override
      public Mono<Void> onClose() {
        return source
            .onClose()
            .doOnTerminate(() -> logger.info("[" + this + "] Source Connection is done"));
      }

      @Override
      public void sendFrame(int streamId, ByteBuf frame) {
        source.sendFrame(streamId, frame);
      }

      @Override
      public void sendErrorAndClose(RSocketErrorException errorException) {
        source.sendErrorAndClose(errorException);
      }

      boolean receivedFirst;

      @Override
      public Flux<ByteBuf> receive() {
        return source
            .receive()
            .doOnSubscribe(
                __ -> logger.warn("Tag {}. Subscribing Connection[{}]", tag, source.hashCode()))
            .doOnNext(
                bb -> {
                  if (!receivedFirst) {
                    receivedFirst = true;
                    disposables.replace(
                        Mono.delay(delay)
                            .takeUntilOther(source.onClose())
                            .subscribe(
                                __ -> {
                                  logger.warn(
                                      "Tag {}. Disposing Connection[{}]", tag, source.hashCode());
                                  source.dispose();
                                }));
                  }
                });
      }

      @Override
      public ByteBufAllocator alloc() {
        return source.alloc();
      }

      @Override
      public SocketAddress remoteAddress() {
        return source.remoteAddress();
      }

      @Override
      public String toString() {
        return "DisconnectingDuplexConnection{"
            + "tag='"
            + tag
            + '\''
            + ", source="
            + source
            + ", disposables="
            + disposables
            + '}';
      }
    }

    private static class ByteBufReleaserOperator
        implements CoreSubscriber<ByteBuf>, Subscription, Fuseable.QueueSubscription<ByteBuf> {

      CoreSubscriber<? super ByteBuf> actual;
      final Sinks.Empty<Void> closeableMonoSink;

      Subscription s;

      public ByteBufReleaserOperator() {
        this.closeableMonoSink = Sinks.unsafe().empty();
      }

      @Override
      public void onSubscribe(Subscription s) {
        if (Operators.validate(this.s, s)) {
          this.s = s;
          actual.onSubscribe(this);
        }
      }

      @Override
      public void onNext(ByteBuf buf) {
        try {
          actual.onNext(buf);
        } finally {
          buf.release();
        }
      }

      Mono<Void> onClose() {
        return closeableMonoSink.asMono();
      }

      @Override
      public void onError(Throwable t) {
        actual.onError(t);
        closeableMonoSink.tryEmitError(t);
      }

      @Override
      public void onComplete() {
        actual.onComplete();
        closeableMonoSink.tryEmitEmpty();
      }

      @Override
      public void request(long n) {
        s.request(n);
      }

      @Override
      public void cancel() {
        s.cancel();
        closeableMonoSink.tryEmitEmpty();
      }

      @Override
      public int requestFusion(int requestedMode) {
        return Fuseable.NONE;
      }

      @Override
      public ByteBuf poll() {
        throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
      }

      @Override
      public int size() {
        throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
      }

      @Override
      public boolean isEmpty() {
        throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
      }

      @Override
      public void clear() {
        throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
      }

      @Override
      public String toString() {
        return "ByteBufReleaserOperator{"
            + "isActualPresent="
            + (actual != null)
            + ", "
            + "isSubscriptionPresent="
            + (s != null)
            + '}';
      }
    }
  }

  class PayloadPredicate implements Predicate<Payload> {
    final int expectedCnt;
    int cnt;

    public PayloadPredicate(int expectedCnt) {
      this.expectedCnt = expectedCnt;
    }

    @Override
    public boolean test(Payload p) {
      boolean shouldConsume = cnt++ < expectedCnt;
      if (!shouldConsume) {
        logger.info(
            "Metadata: \n\r{}\n\rData:{}",
            p.hasMetadata()
                ? new ByteBufRepresentation().fallbackToStringOf(p.sliceMetadata())
                : "Empty",
            new ByteBufRepresentation().fallbackToStringOf(p.sliceData()));
      }
      return shouldConsume;
    }
  }
}
