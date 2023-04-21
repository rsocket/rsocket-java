/*
 * Copyright 2015-2021 the original author or authors.
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

import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;
import static org.assertj.core.api.Assertions.assertThat;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCounted;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.FrameAssert;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.KeepAliveFrameCodec;
import io.rsocket.frame.RequestResponseFrameCodec;
import io.rsocket.test.util.TestClientTransport;
import io.rsocket.test.util.TestDuplexConnection;
import io.rsocket.util.ByteBufPayload;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.retry.Retry;

public class RSocketConnectorTest {

  @ParameterizedTest
  @ValueSource(strings = {"KEEPALIVE", "REQUEST_RESPONSE"})
  public void unexpectedFramesBeforeResumeOKFrame(String frameType) {
    TestClientTransport transport = new TestClientTransport();
    RSocketConnector.create()
        .resume(new Resume().retry(Retry.indefinitely()))
        .connect(transport)
        .block();

    final TestDuplexConnection duplexConnection = transport.testConnection();

    duplexConnection.addToReceivedBuffer(
        KeepAliveFrameCodec.encode(duplexConnection.alloc(), false, 1, Unpooled.EMPTY_BUFFER));
    FrameAssert.assertThat(duplexConnection.pollFrame())
        .typeOf(FrameType.SETUP)
        .hasStreamIdZero()
        .hasNoLeaks();

    FrameAssert.assertThat(duplexConnection.pollFrame()).isNull();

    duplexConnection.dispose();

    final TestDuplexConnection duplexConnection2 = transport.testConnection();

    final ByteBuf frame;
    switch (frameType) {
      case "KEEPALIVE":
        frame =
            KeepAliveFrameCodec.encode(duplexConnection2.alloc(), false, 1, Unpooled.EMPTY_BUFFER);
        break;
      case "REQUEST_RESPONSE":
      default:
        frame =
            RequestResponseFrameCodec.encode(
                duplexConnection2.alloc(), 2, false, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER);
    }
    duplexConnection2.addToReceivedBuffer(frame);

    StepVerifier.create(duplexConnection2.onClose())
        .expectSubscription()
        .expectComplete()
        .verify(Duration.ofSeconds(10));

    FrameAssert.assertThat(duplexConnection2.pollFrame())
        .typeOf(FrameType.RESUME)
        .hasStreamIdZero()
        .hasNoLeaks();

    FrameAssert.assertThat(duplexConnection2.pollFrame())
        .isNotNull()
        .typeOf(FrameType.ERROR)
        .hasData("RESUME_OK frame must be received before any others")
        .hasStreamIdZero()
        .hasNoLeaks();

    transport.alloc().assertHasNoLeaks();
  }

  @Test
  public void ensuresThatSetupPayloadCanBeRetained() {
    AtomicReference<ConnectionSetupPayload> retainedSetupPayload = new AtomicReference<>();
    TestClientTransport transport = new TestClientTransport();

    ByteBuf data = transport.alloc().buffer();

    data.writeCharSequence("data", CharsetUtil.UTF_8);

    RSocketConnector.create()
        .setupPayload(ByteBufPayload.create(data))
        .acceptor(
            (setup, sendingSocket) -> {
              retainedSetupPayload.set(setup.retain());
              return Mono.just(new RSocket() {});
            })
        .connect(transport)
        .block();

    assertThat(transport.testConnection().getSent())
        .hasSize(1)
        .first()
        .matches(
            bb -> {
              DefaultConnectionSetupPayload payload = new DefaultConnectionSetupPayload(bb);
              return !payload.hasMetadata() && payload.getDataUtf8().equals("data");
            })
        .matches(buf -> buf.refCnt() == 2)
        .matches(
            buf -> {
              buf.release();
              return buf.refCnt() == 1;
            });

    ConnectionSetupPayload setup = retainedSetupPayload.get();
    String dataUtf8 = setup.getDataUtf8();
    assertThat("data".equals(dataUtf8) && setup.release()).isTrue();
    assertThat(setup.refCnt()).isZero();

    transport.alloc().assertHasNoLeaks();
  }

  @Test
  public void ensuresThatMonoFromRSocketConnectorCanBeUsedForMultipleSubscriptions() {
    Payload setupPayload = ByteBufPayload.create("TestData", "TestMetadata");
    assertThat(setupPayload.refCnt()).isOne();

    // Keep the data and metadata around so we can try changing them independently
    ByteBuf dataBuf = setupPayload.data();
    ByteBuf metadataBuf = setupPayload.metadata();
    dataBuf.retain();
    metadataBuf.retain();

    TestClientTransport testClientTransport = new TestClientTransport();
    Mono<RSocket> connectionMono =
        RSocketConnector.create().setupPayload(setupPayload).connect(testClientTransport);

    connectionMono
        .as(StepVerifier::create)
        .expectNextCount(1)
        .expectComplete()
        .verify(Duration.ofMillis(100));

    assertThat(testClientTransport.testConnection().getSent())
        .hasSize(1)
        .allMatch(
            bb -> {
              DefaultConnectionSetupPayload payload = new DefaultConnectionSetupPayload(bb);
              return payload.getDataUtf8().equals("TestData")
                  && payload.getMetadataUtf8().equals("TestMetadata");
            })
        .allMatch(ReferenceCounted::release);

    connectionMono
        .as(StepVerifier::create)
        .expectNextCount(1)
        .expectComplete()
        .verify(Duration.ofMillis(100));

    // Changing the original data and metadata should not impact the SetupPayload
    dataBuf.writerIndex(dataBuf.readerIndex());
    dataBuf.writeChar('d');
    dataBuf.release();

    metadataBuf.writerIndex(metadataBuf.readerIndex());
    metadataBuf.writeChar('m');
    metadataBuf.release();

    assertThat(testClientTransport.testConnection().getSent())
        .hasSize(1)
        .allMatch(
            bb -> {
              DefaultConnectionSetupPayload payload = new DefaultConnectionSetupPayload(bb);
              return payload.getDataUtf8().equals("TestData")
                  && payload.getMetadataUtf8().equals("TestMetadata");
            })
        .allMatch(
            byteBuf -> {
              System.out.println("calling release " + byteBuf.refCnt());
              return byteBuf.release();
            });
    assertThat(setupPayload.refCnt()).isZero();

    testClientTransport.alloc().assertHasNoLeaks();
  }

  @Test
  public void ensuresThatSetupPayloadProvidedAsMonoIsReleased() {
    List<Payload> saved = new ArrayList<>();
    AtomicLong subscriptions = new AtomicLong();
    Mono<Payload> setupPayloadMono =
        Mono.create(
            sink -> {
              final long subscriptionN = subscriptions.getAndIncrement();
              Payload payload =
                  ByteBufPayload.create("TestData" + subscriptionN, "TestMetadata" + subscriptionN);
              saved.add(payload);
              sink.success(payload);
            });

    TestClientTransport testClientTransport = new TestClientTransport();
    Mono<RSocket> connectionMono =
        RSocketConnector.create().setupPayload(setupPayloadMono).connect(testClientTransport);

    connectionMono
        .as(StepVerifier::create)
        .expectNextCount(1)
        .expectComplete()
        .verify(Duration.ofMillis(100));

    assertThat(testClientTransport.testConnection().getSent())
        .hasSize(1)
        .allMatch(
            bb -> {
              DefaultConnectionSetupPayload payload = new DefaultConnectionSetupPayload(bb);
              return payload.getDataUtf8().equals("TestData0")
                  && payload.getMetadataUtf8().equals("TestMetadata0");
            })
        .allMatch(ReferenceCounted::release);

    connectionMono
        .as(StepVerifier::create)
        .expectNextCount(1)
        .expectComplete()
        .verify(Duration.ofMillis(100));

    assertThat(testClientTransport.testConnection().getSent())
        .hasSize(1)
        .allMatch(
            bb -> {
              DefaultConnectionSetupPayload payload = new DefaultConnectionSetupPayload(bb);
              return payload.getDataUtf8().equals("TestData1")
                  && payload.getMetadataUtf8().equals("TestMetadata1");
            })
        .allMatch(ReferenceCounted::release);

    assertThat(saved)
        .as("Metadata and data were consumed and released as slices")
        .allMatch(
            payload ->
                payload.refCnt() == 1
                    && payload.data().refCnt() == 0
                    && payload.metadata().refCnt() == 0);

    testClientTransport.alloc().assertHasNoLeaks();
  }

  @Test
  public void ensuresMaxFrameLengthCanNotBeLessThenMtu() {
    RSocketConnector.create()
        .fragment(128)
        .connect(new TestClientTransport().withMaxFrameLength(64))
        .as(StepVerifier::create)
        .expectErrorMessage(
            "Configured maximumTransmissionUnit[128] exceeds configured maxFrameLength[64]")
        .verify();
  }

  @Test
  public void ensuresMaxFrameLengthCanNotBeGreaterThenMaxPayloadSize() {
    RSocketConnector.create()
        .maxInboundPayloadSize(128)
        .connect(new TestClientTransport().withMaxFrameLength(256))
        .as(StepVerifier::create)
        .expectErrorMessage("Configured maxFrameLength[256] exceeds maxPayloadSize[128]")
        .verify();
  }

  @Test
  public void ensuresMaxFrameLengthCanNotBeGreaterThenMaxPossibleFrameLength() {
    RSocketConnector.create()
        .connect(new TestClientTransport().withMaxFrameLength(Integer.MAX_VALUE))
        .as(StepVerifier::create)
        .expectErrorMessage(
            "Configured maxFrameLength["
                + Integer.MAX_VALUE
                + "] "
                + "exceeds maxFrameLength limit "
                + FRAME_LENGTH_MASK)
        .verify();
  }
}
