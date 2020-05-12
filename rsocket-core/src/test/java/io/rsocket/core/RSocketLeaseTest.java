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

package io.rsocket.core;

import static io.rsocket.frame.FrameType.ERROR;
import static io.rsocket.frame.FrameType.SETUP;
import static org.assertj.core.data.Offset.offset;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.TestScheduler;
import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
import io.rsocket.exceptions.Exceptions;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.LeaseFrameCodec;
import io.rsocket.frame.SetupFrameCodec;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.ClientServerInputMultiplexer;
import io.rsocket.lease.*;
import io.rsocket.lease.MissingLeaseException;
import io.rsocket.plugins.InitializingInterceptorRegistry;
import io.rsocket.test.util.TestClientTransport;
import io.rsocket.test.util.TestDuplexConnection;
import io.rsocket.test.util.TestServerTransport;
import io.rsocket.util.DefaultPayload;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Publisher;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class RSocketLeaseTest {
  private static final String TAG = "test";

  private RSocket rSocketRequester;
  private ResponderLeaseHandler responderLeaseHandler;
  private ByteBufAllocator byteBufAllocator;
  private TestDuplexConnection connection;
  private RSocketResponder rSocketResponder;

  private EmitterProcessor<Lease> leaseSender = EmitterProcessor.create();
  private Flux<Lease> leaseReceiver;
  private RequesterLeaseHandler requesterLeaseHandler;

  @BeforeEach
  void setUp() {
    PayloadDecoder payloadDecoder = PayloadDecoder.DEFAULT;
    byteBufAllocator = LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);

    connection = new TestDuplexConnection(byteBufAllocator);
    requesterLeaseHandler = new RequesterLeaseHandler.Impl(TAG, leases -> leaseReceiver = leases);
    responderLeaseHandler =
        new ResponderLeaseHandler.Impl<>(
            TAG, byteBufAllocator, stats -> leaseSender, Optional.empty());

    ClientServerInputMultiplexer multiplexer =
        new ClientServerInputMultiplexer(connection, new InitializingInterceptorRegistry(), true);
    rSocketRequester =
        new RSocketRequester(
            multiplexer.asClientConnection(),
            payloadDecoder,
            StreamIdSupplier.clientSupplier(),
            0,
            0,
            0,
            null,
            requesterLeaseHandler,
            TestScheduler.INSTANCE);

    RSocket mockRSocketHandler = mock(RSocket.class);
    when(mockRSocketHandler.metadataPush(any())).thenReturn(Mono.empty());
    when(mockRSocketHandler.fireAndForget(any())).thenReturn(Mono.empty());
    when(mockRSocketHandler.requestResponse(any())).thenReturn(Mono.empty());
    when(mockRSocketHandler.requestStream(any())).thenReturn(Flux.empty());
    when(mockRSocketHandler.requestChannel(any())).thenReturn(Flux.empty());

    rSocketResponder =
        new RSocketResponder(
            multiplexer.asServerConnection(),
            mockRSocketHandler,
            payloadDecoder,
            responderLeaseHandler,
            0);
  }

  @Test
  public void serverRSocketFactoryRejectsUnsupportedLease() {
    Payload payload = DefaultPayload.create(DefaultPayload.EMPTY_BUFFER);
    ByteBuf setupFrame =
        SetupFrameCodec.encode(
            ByteBufAllocator.DEFAULT,
            true,
            1000,
            30_000,
            "application/octet-stream",
            "application/octet-stream",
            payload);

    TestServerTransport transport = new TestServerTransport();
    RSocketServer.create().bind(transport).block();

    TestDuplexConnection connection = transport.connect();
    connection.addToReceivedBuffer(setupFrame);

    Collection<ByteBuf> sent = connection.getSent();
    Assertions.assertThat(sent).hasSize(1);
    ByteBuf error = sent.iterator().next();
    Assertions.assertThat(FrameHeaderCodec.frameType(error)).isEqualTo(ERROR);
    Assertions.assertThat(Exceptions.from(0, error).getMessage())
        .isEqualTo("lease is not supported");
  }

  @Test
  public void clientRSocketFactorySetsLeaseFlag() {
    TestClientTransport clientTransport = new TestClientTransport();
    RSocketConnector.create().lease(Leases::new).connect(clientTransport).block();

    Collection<ByteBuf> sent = clientTransport.testConnection().getSent();
    Assertions.assertThat(sent).hasSize(1);
    ByteBuf setup = sent.iterator().next();
    Assertions.assertThat(FrameHeaderCodec.frameType(setup)).isEqualTo(SETUP);
    Assertions.assertThat(SetupFrameCodec.honorLease(setup)).isTrue();
  }

  @ParameterizedTest
  @MethodSource("interactions")
  void requesterMissingLeaseRequestsAreRejected(Function<RSocket, Publisher<?>> interaction) {
    Assertions.assertThat(rSocketRequester.availability()).isCloseTo(0.0, offset(1e-2));

    StepVerifier.create(interaction.apply(rSocketRequester))
        .expectError(MissingLeaseException.class)
        .verify(Duration.ofSeconds(5));
  }

  @ParameterizedTest
  @MethodSource("interactions")
  void requesterPresentLeaseRequestsAreAccepted(Function<RSocket, Publisher<?>> interaction) {
    requesterLeaseHandler.receive(leaseFrame(5_000, 2, Unpooled.EMPTY_BUFFER));

    Assertions.assertThat(rSocketRequester.availability()).isCloseTo(1.0, offset(1e-2));

    Flux.from(interaction.apply(rSocketRequester))
        .take(Duration.ofMillis(500))
        .as(StepVerifier::create)
        .expectComplete()
        .verify(Duration.ofSeconds(5));
    Assertions.assertThat(rSocketRequester.availability()).isCloseTo(0.5, offset(1e-2));
  }

  @ParameterizedTest
  @MethodSource("interactions")
  void requesterDepletedAllowedLeaseRequestsAreRejected(
      Function<RSocket, Publisher<?>> interaction) {
    requesterLeaseHandler.receive(leaseFrame(5_000, 1, Unpooled.EMPTY_BUFFER));
    interaction.apply(rSocketRequester);

    Flux.from(interaction.apply(rSocketRequester))
        .as(StepVerifier::create)
        .expectError(MissingLeaseException.class)
        .verify(Duration.ofSeconds(5));

    Assertions.assertThat(rSocketRequester.availability()).isCloseTo(0.0, offset(1e-2));
  }

  @ParameterizedTest
  @MethodSource("interactions")
  void requesterExpiredLeaseRequestsAreRejected(Function<RSocket, Publisher<?>> interaction) {
    requesterLeaseHandler.receive(leaseFrame(50, 1, Unpooled.EMPTY_BUFFER));

    Flux.defer(() -> interaction.apply(rSocketRequester))
        .delaySubscription(Duration.ofMillis(200))
        .as(StepVerifier::create)
        .expectError(MissingLeaseException.class)
        .verify(Duration.ofSeconds(5));
  }

  @Test
  void requesterAvailabilityRespectsTransport() {
    requesterLeaseHandler.receive(leaseFrame(5_000, 1, Unpooled.EMPTY_BUFFER));
    double unavailable = 0.0;
    connection.setAvailability(unavailable);
    Assertions.assertThat(rSocketRequester.availability()).isCloseTo(unavailable, offset(1e-2));
  }

  @ParameterizedTest
  @MethodSource("interactions")
  void responderMissingLeaseRequestsAreRejected(Function<RSocket, Publisher<?>> interaction) {
    StepVerifier.create(interaction.apply(rSocketResponder))
        .expectError(MissingLeaseException.class)
        .verify(Duration.ofSeconds(5));
  }

  @ParameterizedTest
  @MethodSource("interactions")
  void responderPresentLeaseRequestsAreAccepted(Function<RSocket, Publisher<?>> interaction) {
    leaseSender.onNext(Lease.create(5_000, 2));

    Flux.from(interaction.apply(rSocketResponder))
        .take(Duration.ofMillis(500))
        .as(StepVerifier::create)
        .expectComplete()
        .verify(Duration.ofSeconds(5));
  }

  @ParameterizedTest
  @MethodSource("interactions")
  void responderDepletedAllowedLeaseRequestsAreRejected(
      Function<RSocket, Publisher<?>> interaction) {
    leaseSender.onNext(Lease.create(5_000, 1));

    Flux<?> responder = Flux.from(interaction.apply(rSocketResponder));
    responder.subscribe();
    Flux.from(interaction.apply(rSocketResponder))
        .as(StepVerifier::create)
        .expectError(MissingLeaseException.class)
        .verify(Duration.ofSeconds(5));
  }

  @ParameterizedTest
  @MethodSource("interactions")
  void expiredLeaseRequestsAreRejected(Function<RSocket, Publisher<?>> interaction) {
    leaseSender.onNext(Lease.create(50, 1));

    Flux.from(interaction.apply(rSocketRequester))
        .delaySubscription(Duration.ofMillis(100))
        .as(StepVerifier::create)
        .expectError(MissingLeaseException.class)
        .verify(Duration.ofSeconds(5));
  }

  @Test
  void sendLease() {
    ByteBuf metadata = byteBufAllocator.buffer();
    Charset utf8 = StandardCharsets.UTF_8;
    String metadataContent = "test";
    metadata.writeCharSequence(metadataContent, utf8);
    int ttl = 5_000;
    int numberOfRequests = 2;
    leaseSender.onNext(Lease.create(5_000, 2, metadata));

    ByteBuf leaseFrame =
        connection
            .getSent()
            .stream()
            .filter(f -> FrameHeaderCodec.frameType(f) == FrameType.LEASE)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Lease frame not sent"));

    Assertions.assertThat(LeaseFrameCodec.ttl(leaseFrame)).isEqualTo(ttl);
    Assertions.assertThat(LeaseFrameCodec.numRequests(leaseFrame)).isEqualTo(numberOfRequests);
    Assertions.assertThat(LeaseFrameCodec.metadata(leaseFrame).toString(utf8))
        .isEqualTo(metadataContent);
  }

  @Test
  void receiveLease() {
    Collection<Lease> receivedLeases = new ArrayList<>();
    leaseReceiver.subscribe(lease -> receivedLeases.add(lease));

    ByteBuf metadata = byteBufAllocator.buffer();
    Charset utf8 = StandardCharsets.UTF_8;
    String metadataContent = "test";
    metadata.writeCharSequence(metadataContent, utf8);
    int ttl = 5_000;
    int numberOfRequests = 2;

    ByteBuf leaseFrame = leaseFrame(ttl, numberOfRequests, metadata).retain(1);

    connection.addToReceivedBuffer(leaseFrame);

    Assertions.assertThat(receivedLeases.isEmpty()).isFalse();
    Lease receivedLease = receivedLeases.iterator().next();
    Assertions.assertThat(receivedLease.getTimeToLiveMillis()).isEqualTo(ttl);
    Assertions.assertThat(receivedLease.getStartingAllowedRequests()).isEqualTo(numberOfRequests);
    Assertions.assertThat(receivedLease.getMetadata().toString(utf8)).isEqualTo(metadataContent);
  }

  ByteBuf leaseFrame(int ttl, int requests, ByteBuf metadata) {
    return LeaseFrameCodec.encode(byteBufAllocator, ttl, requests, metadata);
  }

  static Stream<Function<RSocket, Publisher<?>>> interactions() {
    return Stream.of(
        rSocket -> rSocket.fireAndForget(DefaultPayload.create("test")),
        rSocket -> rSocket.requestResponse(DefaultPayload.create("test")),
        rSocket -> rSocket.requestStream(DefaultPayload.create("test")),
        rSocket -> rSocket.requestChannel(Mono.just(DefaultPayload.create("test"))));
  }
}
