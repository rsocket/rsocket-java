package io.rsocket.core;

import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;
import static org.assertj.core.api.Assertions.assertThat;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.FrameAssert;
import io.rsocket.RSocket;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.KeepAliveFrameCodec;
import io.rsocket.frame.RequestResponseFrameCodec;
import io.rsocket.test.util.TestDuplexConnection;
import io.rsocket.test.util.TestServerTransport;
import java.time.Duration;
import java.util.Random;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.test.StepVerifier;

public class RSocketServerTest {

  @Test
  public void unexpectedFramesBeforeSetupFrame() {
    TestServerTransport transport = new TestServerTransport();
    RSocketServer.create().bind(transport).block();

    final TestDuplexConnection duplexConnection = transport.connect();

    duplexConnection.addToReceivedBuffer(
        KeepAliveFrameCodec.encode(duplexConnection.alloc(), false, 1, Unpooled.EMPTY_BUFFER));

    StepVerifier.create(duplexConnection.onClose())
        .expectSubscription()
        .expectComplete()
        .verify(Duration.ofSeconds(10));

    FrameAssert.assertThat(duplexConnection.pollFrame())
        .isNotNull()
        .typeOf(FrameType.ERROR)
        .hasData("SETUP or RESUME frame must be received before any others")
        .hasStreamIdZero()
        .hasNoLeaks();
  }

  @Test
  public void ensuresMaxFrameLengthCanNotBeLessThenMtu() {
    RSocketServer.create()
        .fragment(128)
        .bind(new TestServerTransport().withMaxFrameLength(64))
        .as(StepVerifier::create)
        .expectErrorMessage(
            "Configured maximumTransmissionUnit[128] exceeds configured maxFrameLength[64]")
        .verify();
  }

  @Test
  public void ensuresMaxFrameLengthCanNotBeGreaterThenMaxPayloadSize() {
    RSocketServer.create()
        .maxInboundPayloadSize(128)
        .bind(new TestServerTransport().withMaxFrameLength(256))
        .as(StepVerifier::create)
        .expectErrorMessage("Configured maxFrameLength[256] exceeds maxPayloadSize[128]")
        .verify();
  }

  @Test
  public void ensuresMaxFrameLengthCanNotBeGreaterThenMaxPossibleFrameLength() {
    RSocketServer.create()
        .bind(new TestServerTransport().withMaxFrameLength(Integer.MAX_VALUE))
        .as(StepVerifier::create)
        .expectErrorMessage(
            "Configured maxFrameLength["
                + Integer.MAX_VALUE
                + "] "
                + "exceeds maxFrameLength limit "
                + FRAME_LENGTH_MASK)
        .verify();
  }

  @Test
  public void unexpectedFramesBeforeSetup() {
    MonoProcessor<Void> connectedMono = MonoProcessor.create();

    TestServerTransport transport = new TestServerTransport();
    RSocketServer.create()
        .acceptor(
            (setup, sendingSocket) -> {
              connectedMono.onComplete();
              return Mono.just(new RSocket() {});
            })
        .bind(transport)
        .block();

    byte[] bytes = new byte[16_000_000];
    new Random().nextBytes(bytes);

    TestDuplexConnection connection = transport.connect();
    connection.addToReceivedBuffer(
        RequestResponseFrameCodec.encode(
            ByteBufAllocator.DEFAULT,
            1,
            false,
            Unpooled.EMPTY_BUFFER,
            ByteBufAllocator.DEFAULT.buffer(bytes.length).writeBytes(bytes)));

    StepVerifier.create(connection.onClose()).expectComplete().verify(Duration.ofSeconds(30));
    assertThat(connectedMono.isTerminated()).as("Connection should not succeed").isFalse();
  }
}
