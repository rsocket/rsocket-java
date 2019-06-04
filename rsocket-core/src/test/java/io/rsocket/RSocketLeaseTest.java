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

package io.rsocket;

import static io.rsocket.frame.FrameType.ERROR;
import static io.rsocket.frame.FrameType.SETUP;
import static org.assertj.core.data.Offset.offset;
import static org.mockito.Mockito.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.rsocket.exceptions.Exceptions;
import io.rsocket.exceptions.MissingLeaseException;
import io.rsocket.frame.FrameHeaderFlyweight;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.LeaseFrameFlyweight;
import io.rsocket.frame.SetupFrameFlyweight;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.lease.Lease;
import io.rsocket.lease.LeaseHandler;
import io.rsocket.lease.Leases;
import io.rsocket.test.util.TestClientTransport;
import io.rsocket.test.util.TestDuplexConnection;
import io.rsocket.test.util.TestServerTransport;
import io.rsocket.util.DefaultPayload;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class RSocketLeaseTest {
  private RSocketRequester rSocketRequester;
  private LeaseHandler.Responder responderLeaseHandler;
  private Leases leases;
  private ByteBufAllocator byteBufAllocator;
  private TestDuplexConnection connection;
  private RSocketResponder rSocketResponder;

  @BeforeEach
  void setUp() {
    connection = new TestDuplexConnection();
    PayloadDecoder payloadDecoder = PayloadDecoder.DEFAULT;
    byteBufAllocator = UnpooledByteBufAllocator.DEFAULT;
    LeaseHandler leaseHandler = new LeaseHandler("test", byteBufAllocator);
    LeaseHandler.Requester requesterLeaseHandler = leaseHandler.requester();
    responderLeaseHandler = leaseHandler.responder();
    leases = leaseHandler.leaseSender();

    rSocketRequester =
        new RSocketRequester(
            byteBufAllocator,
            connection,
            payloadDecoder,
            err -> {},
            StreamIdSupplier.clientSupplier(),
            requesterLeaseHandler);

    RSocket mockRSocketHandler = mock(RSocket.class);
    when(mockRSocketHandler.metadataPush(any())).thenReturn(Mono.empty());
    when(mockRSocketHandler.fireAndForget(any())).thenReturn(Mono.empty());
    when(mockRSocketHandler.requestResponse(any())).thenReturn(Mono.empty());
    when(mockRSocketHandler.requestStream(any())).thenReturn(Flux.empty());
    when(mockRSocketHandler.requestChannel(any())).thenReturn(Flux.empty());

    rSocketResponder =
        new RSocketResponder(
            byteBufAllocator,
            connection,
            mockRSocketHandler,
            payloadDecoder,
            err -> {},
            responderLeaseHandler);
  }

  @Test
  public void serverRSocketFactoryRejectsUnsupportedLease() {
    Payload payload = DefaultPayload.create(DefaultPayload.EMPTY_BUFFER);
    ByteBuf setupFrame =
        SetupFrameFlyweight.encode(
            ByteBufAllocator.DEFAULT,
            true,
            1000,
            30_000,
            "application/octet-stream",
            "application/octet-stream",
            payload.sliceMetadata(),
            payload.sliceData());

    TestServerTransport transport = new TestServerTransport();
    Closeable server =
        RSocketFactory.receive()
            .acceptor((setup, sendingSocket) -> Mono.just(new AbstractRSocket() {}))
            .transport(transport)
            .start()
            .block();

    TestDuplexConnection connection = transport.connect();
    connection.addToReceivedBuffer(setupFrame);

    Collection<ByteBuf> sent = connection.getSent();
    Assertions.assertThat(sent).hasSize(1);
    ByteBuf error = sent.iterator().next();
    Assertions.assertThat(FrameHeaderFlyweight.frameType(error)).isEqualTo(ERROR);
    Assertions.assertThat(Exceptions.from(error).getMessage()).isEqualTo("lease is not supported");
  }

  @Test
  public void clientRSocketFactorySetsLeaseFlag() {
    TestClientTransport clientTransport = new TestClientTransport();
    RSocketFactory.connect()
        .lease((leaseSender, leaseConfig) -> {})
        .transport(clientTransport)
        .start()
        .block();

    Collection<ByteBuf> sent = clientTransport.testConnection().getSent();
    Assertions.assertThat(sent).hasSize(1);
    ByteBuf setup = sent.iterator().next();
    Assertions.assertThat(FrameHeaderFlyweight.frameType(setup)).isEqualTo(SETUP);
    Assertions.assertThat(SetupFrameFlyweight.honorLease(setup)).isTrue();
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
    responderLeaseHandler.onReceiveLease(5_000, 2, Unpooled.EMPTY_BUFFER);

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
    responderLeaseHandler.onReceiveLease(5_000, 1, Unpooled.EMPTY_BUFFER);

    Flux<?> requester = Flux.from(interaction.apply(rSocketRequester));
    requester.subscribe();

    requester
        .as(StepVerifier::create)
        .expectError(MissingLeaseException.class)
        .verify(Duration.ofSeconds(5));

    Assertions.assertThat(rSocketRequester.availability()).isCloseTo(0.0, offset(1e-2));
  }

  @ParameterizedTest
  @MethodSource("interactions")
  void requesterExpiredLeaseRequestsAreRejected(Function<RSocket, Publisher<?>> interaction) {
    responderLeaseHandler.onReceiveLease(50, 1, Unpooled.EMPTY_BUFFER);

    Flux.from(interaction.apply(rSocketRequester))
        .delaySubscription(Duration.ofMillis(100))
        .as(StepVerifier::create)
        .expectError(MissingLeaseException.class)
        .verify(Duration.ofSeconds(5));
  }

  @ParameterizedTest
  @MethodSource("interactions")
  void requesterMultipleSubscriptions(Function<RSocket, Publisher<?>> interaction) {
    responderLeaseHandler.onReceiveLease(30_000, 2, Unpooled.EMPTY_BUFFER);
    Flux<?> call = Flux.from(interaction.apply(rSocketRequester));
    Assertions.assertThat(rSocketRequester.availability()).isCloseTo(0.5, offset(1e-2));
    call.subscribe();
    Assertions.assertThat(rSocketRequester.availability()).isCloseTo(0.5, offset(1e-2));
    call.subscribe();
    Assertions.assertThat(rSocketRequester.availability()).isCloseTo(0.0, offset(1e-2));
    call.as(StepVerifier::create)
        .expectError(MissingLeaseException.class)
        .verify(Duration.ofSeconds(5));
  }

  @Test
  void requesterAvailabilityRespectsTransport() {
    responderLeaseHandler.onReceiveLease(5_000, 1, Unpooled.EMPTY_BUFFER);

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
    leases.sendLease(5_000, 2);

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
    leases.sendLease(5_000, 1);

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
    leases.sendLease(50, 1);

    Flux.from(interaction.apply(rSocketRequester))
        .delaySubscription(Duration.ofMillis(100))
        .as(StepVerifier::create)
        .expectError(MissingLeaseException.class)
        .verify(Duration.ofSeconds(5));
  }

  @Test
  void leaseSenderInitialLeaseIsEmpty() {
    Lease lease = leases.responderLease();
    Assertions.assertThat(lease.isEmpty()).isTrue();
  }

  @Test
  void sendLease() {
    ByteBuf metadata = byteBufAllocator.buffer();
    Charset utf8 = StandardCharsets.UTF_8;
    String metadataContent = "test";
    metadata.writeCharSequence(metadataContent, utf8);
    int ttl = 5_000;
    int numberOfRequests = 2;
    leases.sendLease(ttl, numberOfRequests, metadata);
    Lease responderLease = leases.responderLease();

    Assertions.assertThat(responderLease.getTimeToLiveMillis()).isEqualTo(ttl);
    Assertions.assertThat(responderLease.getAllowedRequests()).isEqualTo(numberOfRequests);
    Assertions.assertThat(responderLease.getMetadata().toString(utf8)).isEqualTo(metadataContent);

    ByteBuf leaseFrame =
        connection
            .getSent()
            .stream()
            .filter(f -> FrameHeaderFlyweight.frameType(f) == FrameType.LEASE)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Lease frame not sent"));

    Assertions.assertThat(LeaseFrameFlyweight.ttl(leaseFrame)).isEqualTo(ttl);
    Assertions.assertThat(LeaseFrameFlyweight.numRequests(leaseFrame)).isEqualTo(numberOfRequests);
    Assertions.assertThat(LeaseFrameFlyweight.metadata(leaseFrame).toString(utf8))
        .isEqualTo(metadataContent);
  }

  @Test
  void receiveLease() {
    Collection<LeaseState> receivedLeases = new ArrayList<>();

    leases.receiveLease(
        (timeToLiveMillis, numberOfRequests, metadata) ->
            receivedLeases.add(new LeaseState(timeToLiveMillis, numberOfRequests, metadata)));

    ByteBuf metadata = byteBufAllocator.buffer();
    Charset utf8 = StandardCharsets.UTF_8;
    String metadataContent = "test";
    metadata.writeCharSequence(metadataContent, utf8);
    int ttl = 5_000;
    int numberOfRequests = 2;

    ByteBuf leaseFrame =
        LeaseFrameFlyweight.encode(byteBufAllocator, ttl, numberOfRequests, metadata);

    connection.addToReceivedBuffer(leaseFrame);

    Assertions.assertThat(receivedLeases.isEmpty()).isFalse();
    LeaseState receivedLease = receivedLeases.iterator().next();
    Assertions.assertThat(receivedLease.getTtl()).isEqualTo(ttl);
    Assertions.assertThat(receivedLease.getRequests()).isEqualTo(numberOfRequests);
    Assertions.assertThat(receivedLease.getMetadata().toString(utf8)).isEqualTo(metadataContent);
  }

  static Stream<Function<RSocket, Publisher<?>>> interactions() {
    return Stream.of(
        rSocket -> rSocket.fireAndForget(DefaultPayload.create("test")),
        rSocket -> rSocket.requestResponse(DefaultPayload.create("test")),
        rSocket -> rSocket.requestStream(DefaultPayload.create("test")),
        rSocket -> rSocket.requestChannel(Mono.just(DefaultPayload.create("test"))));
  }

  static class LeaseState {
    private final int ttl;
    private final int requests;
    private final ByteBuf metadata;

    public LeaseState(int ttl, int requests, ByteBuf metadata) {
      this.ttl = ttl;
      this.requests = requests;
      this.metadata = metadata;
    }

    public int getTtl() {
      return ttl;
    }

    public int getRequests() {
      return requests;
    }

    public ByteBuf getMetadata() {
      return metadata;
    }
  }
}
