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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import io.rsocket.exceptions.ConnectionErrorException;
import io.rsocket.frame.FrameHeaderFlyweight;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.KeepAliveFrameFlyweight;
import io.rsocket.frame.SetupFrameFlyweight;
import io.rsocket.internal.KeepAliveData;
import io.rsocket.keepalive.KeepAliveConnection;
import io.rsocket.resume.ResumeStateHolder;
import io.rsocket.test.util.TestDuplexConnection;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

public class KeepAliveTest {
  private static final int TICK_PERIOD = 100;
  private static final int TIMEOUT = 700;

  private TestDuplexConnection testConnection;
  private Function<ByteBuf, Mono<KeepAliveData>> timingsProvider;
  private List<Throwable> errors;
  private Consumer<Throwable> errorConsumer;
  private KeepAliveConnection clientConnection;
  private KeepAliveConnection serverConnection;
  private ByteBufAllocator allocator;

  @BeforeEach
  void setUp() {
    allocator = ByteBufAllocator.DEFAULT;
    testConnection = new TestDuplexConnection();

    timingsProvider = f -> Mono.just(new KeepAliveData(TICK_PERIOD, TIMEOUT));
    errors = new ArrayList<>();
    errorConsumer = errors::add;

    clientConnection =
        KeepAliveConnection.ofClient(allocator, testConnection, timingsProvider, errorConsumer);
    serverConnection =
        KeepAliveConnection.ofServer(allocator, testConnection, timingsProvider, errorConsumer);
  }

  @Test
  void clientNoFramesBeforeSetup() {

    Mono.delay(Duration.ofSeconds(1)).block();

    Assertions.assertThat(errors).isEmpty();
    Assertions.assertThat(clientConnection.isDisposed()).isFalse();
    Assertions.assertThat(clientConnection.availability()).isEqualTo(testConnection.availability());
    Assertions.assertThat(testConnection.getSent()).isEmpty();
  }

  @Test
  void clientFramesAfterSetup() {
    clientConnection.sendOne(setupFrame()).subscribe();

    Mono.delay(Duration.ofMillis(500)).block();

    Assertions.assertThat(errors).isEmpty();
    Assertions.assertThat(clientConnection.isDisposed()).isFalse();
    Collection<ByteBuf> sent = testConnection.getSent();
    Collection<ByteBuf> sentAfterSetup =
        sent.stream().filter(f -> frameType(f) != FrameType.SETUP).collect(Collectors.toList());

    Assertions.assertThat(sentAfterSetup).isNotEmpty();
    sentAfterSetup.forEach(
        f -> {
          Assertions.assertThat(frameType(f)).isEqualTo(FrameType.KEEPALIVE);
          Assertions.assertThat(KeepAliveFrameFlyweight.respondFlag(f)).isEqualTo(true);
          Assertions.assertThat(KeepAliveFrameFlyweight.lastPosition(f)).isEqualTo(0);
        });

    sent.forEach(ReferenceCountUtil::safeRelease);
  }

  @Test
  void clientCloseOnMissingKeepalives() {
    clientConnection.sendOne(setupFrame()).subscribe();

    Mono.delay(Duration.ofSeconds(1)).block();

    Assertions.assertThat(errors).hasSize(1);
    Throwable err = errors.get(0);
    Assertions.assertThat(err).isExactlyInstanceOf(ConnectionErrorException.class);
    Assertions.assertThat(err.getMessage()).isEqualTo("No keep-alive acks for 700 ms");
    Assertions.assertThat(clientConnection.isDisposed()).isTrue();

    testConnection.getSent().forEach(ReferenceCountUtil::safeRelease);
  }

  @Test
  void clientResumptionState() {
    TestResumeStateHolder resumeStateHolder = new TestResumeStateHolder();
    clientConnection.acceptResumeState(resumeStateHolder);

    clientConnection.sendOne(setupFrame()).subscribe();
    clientConnection.receive().subscribe();

    testConnection.addToReceivedBuffer(keepAliveFrame(false, 1));
    testConnection.addToReceivedBuffer(keepAliveFrame(false, 2));
    testConnection.addToReceivedBuffer(keepAliveFrame(false, 3));

    Mono.delay(Duration.ofMillis(500)).block();

    List<Long> receivedPositions = resumeStateHolder.receivedImpliedPositions();

    Collection<ByteBuf> sent = testConnection.getSent();
    List<Long> sentPositions =
        sent.stream()
            .filter(f -> frameType(f) != FrameType.SETUP)
            .map(KeepAliveFrameFlyweight::lastPosition)
            .limit(4)
            .collect(Collectors.toList());

    Assertions.assertThat(sentPositions).isEqualTo(Arrays.asList(1L, 5L, 6L, 8L));
    Assertions.assertThat(receivedPositions).isEqualTo(Arrays.asList(1L, 2L, 3L));

    sent.forEach(ReferenceCountUtil::safeRelease);
  }

  @Test
  void serverFrames() {
    serverConnection.sendOne(setupFrame()).subscribe();
    serverConnection.receive().subscribe();
    testConnection.addToReceivedBuffer(keepAliveFrame(true, 0));

    Assertions.assertThat(errors).isEmpty();
    Assertions.assertThat(clientConnection.isDisposed()).isFalse();

    Collection<ByteBuf> sent = testConnection.getSent();
    Collection<ByteBuf> sentAfterSetup =
        sent.stream().filter(f -> frameType(f) != FrameType.SETUP).collect(Collectors.toList());
    Assertions.assertThat(sentAfterSetup).isNotEmpty();

    sentAfterSetup.forEach(
        f -> {
          Assertions.assertThat(frameType(f)).isEqualTo(FrameType.KEEPALIVE);
          Assertions.assertThat(KeepAliveFrameFlyweight.respondFlag(f)).isEqualTo(false);
          Assertions.assertThat(KeepAliveFrameFlyweight.lastPosition(f)).isEqualTo(0);
        });

    sent.forEach(ReferenceCountUtil::safeRelease);
  }

  @Test
  void serverCloseOnMissingKeepalives() {
    serverConnection.sendOne(setupFrame()).subscribe();

    Mono.delay(Duration.ofSeconds(1)).block();

    Assertions.assertThat(errors).hasSize(1);
    Throwable err = errors.get(0);
    Assertions.assertThat(err).isExactlyInstanceOf(ConnectionErrorException.class);
    Assertions.assertThat(err.getMessage()).isEqualTo("No keep-alive acks for 700 ms");
    Assertions.assertThat(clientConnection.isDisposed()).isTrue();

    testConnection.getSent().forEach(ReferenceCountUtil::safeRelease);
  }

  private ByteBuf keepAliveFrame(boolean respond, int pos) {
    return KeepAliveFrameFlyweight.encode(allocator, respond, pos, Unpooled.EMPTY_BUFFER);
  }

  private ByteBuf setupFrame() {
    return SetupFrameFlyweight.encode(
        allocator,
        false,
        TICK_PERIOD,
        TIMEOUT,
        "metadataType",
        "dataType",
        byteBuf("metadata"),
        byteBuf("data"));
  }

  private ByteBuf byteBuf(String msg) {
    return Unpooled.wrappedBuffer(msg.getBytes(StandardCharsets.UTF_8));
  }

  private static FrameType frameType(ByteBuf frame) {
    return FrameHeaderFlyweight.frameType(frame);
  }

  private static class TestResumeStateHolder implements ResumeStateHolder {
    private final List<Long> sentPositions = Arrays.asList(1L, 5L, 6L, 8L);
    private final List<Long> receivedPositions = new ArrayList<>();
    private int counter = 0;

    @Override
    public long impliedPosition() {
      long res = sentPositions.get(counter);
      counter = Math.min(counter + 1, sentPositions.size() - 1);
      return res;
    }

    @Override
    public void onImpliedPosition(long remoteImpliedPos) {
      receivedPositions.add(remoteImpliedPos);
    }

    public List<Long> receivedImpliedPositions() {
      return receivedPositions;
    }
  }
}
