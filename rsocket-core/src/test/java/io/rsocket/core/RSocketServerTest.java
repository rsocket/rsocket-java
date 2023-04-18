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

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.Closeable;
import io.rsocket.FrameAssert;
import io.rsocket.RSocket;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.KeepAliveFrameCodec;
import io.rsocket.frame.RequestResponseFrameCodec;
import io.rsocket.test.util.TestDuplexConnection;
import io.rsocket.test.util.TestServerTransport;
import java.time.Duration;
import java.util.Random;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;

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
    duplexConnection.alloc().assertHasNoLeaks();
  }

  @Test
  public void timeoutOnNoFirstFrame() {
    final VirtualTimeScheduler scheduler = VirtualTimeScheduler.getOrSet();
    TestServerTransport transport = new TestServerTransport();
    try {
      RSocketServer.create().maxTimeToFirstFrame(Duration.ofMinutes(2)).bind(transport).block();

      final TestDuplexConnection duplexConnection = transport.connect();

      scheduler.advanceTimeBy(Duration.ofMinutes(1));

      Assertions.assertThat(duplexConnection.isDisposed()).isFalse();

      scheduler.advanceTimeBy(Duration.ofMinutes(1));

      StepVerifier.create(duplexConnection.onClose())
          .expectSubscription()
          .expectComplete()
          .verify(Duration.ofSeconds(10));

      FrameAssert.assertThat(duplexConnection.pollFrame()).isNull();
    } finally {
      transport.alloc().assertHasNoLeaks();
      VirtualTimeScheduler.reset();
    }
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
    Sinks.Empty<Void> connectedSink = Sinks.empty();

    TestServerTransport transport = new TestServerTransport();
    Closeable server =
        RSocketServer.create()
            .acceptor(
                (setup, sendingSocket) -> {
                  connectedSink.tryEmitEmpty();
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
    assertThat(connectedSink.scan(Scannable.Attr.TERMINATED))
        .as("Connection should not succeed")
        .isFalse();
    FrameAssert.assertThat(connection.pollFrame())
        .hasStreamIdZero()
        .hasData("SETUP or RESUME frame must be received before any others")
        .hasNoLeaks();
    server.dispose();
    transport.alloc().assertHasNoLeaks();
  }
}
