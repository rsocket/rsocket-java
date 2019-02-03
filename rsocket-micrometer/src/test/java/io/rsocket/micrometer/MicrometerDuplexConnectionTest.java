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

package io.rsocket.micrometer;

import static io.netty.buffer.UnpooledByteBufAllocator.DEFAULT;
import static io.rsocket.frame.FrameType.CANCEL;
import static io.rsocket.frame.FrameType.COMPLETE;
import static io.rsocket.frame.FrameType.ERROR;
import static io.rsocket.frame.FrameType.KEEPALIVE;
import static io.rsocket.frame.FrameType.LEASE;
import static io.rsocket.frame.FrameType.METADATA_PUSH;
import static io.rsocket.frame.FrameType.REQUEST_CHANNEL;
import static io.rsocket.frame.FrameType.REQUEST_FNF;
import static io.rsocket.frame.FrameType.REQUEST_N;
import static io.rsocket.frame.FrameType.REQUEST_RESPONSE;
import static io.rsocket.frame.FrameType.REQUEST_STREAM;
import static io.rsocket.frame.FrameType.RESUME;
import static io.rsocket.frame.FrameType.RESUME_OK;
import static io.rsocket.frame.FrameType.SETUP;
import static io.rsocket.plugins.DuplexConnectionInterceptor.Type.CLIENT;
import static io.rsocket.plugins.DuplexConnectionInterceptor.Type.SERVER;
import static io.rsocket.test.TestFrames.createTestCancelFrame;
import static io.rsocket.test.TestFrames.createTestErrorFrame;
import static io.rsocket.test.TestFrames.createTestKeepaliveFrame;
import static io.rsocket.test.TestFrames.createTestLeaseFrame;
import static io.rsocket.test.TestFrames.createTestMetadataPushFrame;
import static io.rsocket.test.TestFrames.createTestPayloadFrame;
import static io.rsocket.test.TestFrames.createTestRequestChannelFrame;
import static io.rsocket.test.TestFrames.createTestRequestFireAndForgetFrame;
import static io.rsocket.test.TestFrames.createTestRequestNFrame;
import static io.rsocket.test.TestFrames.createTestRequestResponseFrame;
import static io.rsocket.test.TestFrames.createTestRequestStreamFrame;
import static io.rsocket.test.TestFrames.createTestResumeFrame;
import static io.rsocket.test.TestFrames.createTestResumeOkFrame;
import static io.rsocket.test.TestFrames.createTestSetupFrame;
import static io.rsocket.util.AbstractionLeakingFrameUtils.toAbstractionLeakingFrame;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import io.rsocket.frame.FrameType;
import io.rsocket.plugins.DuplexConnectionInterceptor.Type;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.test.StepVerifier;

// TODO: Flyweight Frames don't support EXT frames, so can't be tested today
final class MicrometerDuplexConnectionTest {

  private final DuplexConnection delegate = mock(DuplexConnection.class, RETURNS_SMART_NULLS);

  private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

  @DisplayName("constructor throws NullPointerException with null connectionType")
  @Test
  void constructorNullConnectionType() {
    assertThatNullPointerException()
        .isThrownBy(() -> new MicrometerDuplexConnection(null, delegate, meterRegistry))
        .withMessage("connectionType must not be null");
  }

  @DisplayName("constructor throws NullPointerException with null delegate")
  @Test
  void constructorNullDelegate() {
    assertThatNullPointerException()
        .isThrownBy(() -> new MicrometerDuplexConnection(CLIENT, null, meterRegistry))
        .withMessage("delegate must not be null");
  }

  @DisplayName("constructor throws NullPointerException with null meterRegistry")
  @Test
  void constructorNullMeterRegistry() {

    assertThatNullPointerException()
        .isThrownBy(() -> new MicrometerDuplexConnection(CLIENT, delegate, null))
        .withMessage("meterRegistry must not be null");
  }

  @DisplayName("dispose gathers metrics")
  @Test
  void dispose() {
    new MicrometerDuplexConnection(
            CLIENT, delegate, meterRegistry, Tag.of("test-key", "test-value"))
        .dispose();

    assertThat(
            meterRegistry
                .get("rsocket.duplex.connection.dispose")
                .tag("connection.type", CLIENT.name())
                .tag("test-key", "test-value")
                .counter()
                .count())
        .isEqualTo(1);
  }

  @DisplayName("onClose gathers metrics")
  @Test
  void onClose() {
    when(delegate.onClose()).thenReturn(Mono.empty());

    new MicrometerDuplexConnection(
            CLIENT, delegate, meterRegistry, Tag.of("test-key", "test-value"))
        .onClose()
        .subscribe(Operators.drainSubscriber());

    assertThat(
            meterRegistry
                .get("rsocket.duplex.connection.close")
                .tag("connection.type", CLIENT.name())
                .tag("test-key", "test-value")
                .counter()
                .count())
        .isEqualTo(1);
  }

  @DisplayName("receive gathers metrics")
  @Test
  void receive() {
    Flux<Frame> frames =
        Flux.just(
            toAbstractionLeakingFrame(DEFAULT, 1, createTestCancelFrame()),
            toAbstractionLeakingFrame(DEFAULT, 1, createTestErrorFrame()),
            toAbstractionLeakingFrame(DEFAULT, 1, createTestKeepaliveFrame()),
            toAbstractionLeakingFrame(DEFAULT, 1, createTestLeaseFrame()),
            toAbstractionLeakingFrame(DEFAULT, 1, createTestMetadataPushFrame()),
            toAbstractionLeakingFrame(DEFAULT, 1, createTestPayloadFrame()),
            toAbstractionLeakingFrame(DEFAULT, 1, createTestRequestChannelFrame()),
            toAbstractionLeakingFrame(DEFAULT, 1, createTestRequestFireAndForgetFrame()),
            toAbstractionLeakingFrame(DEFAULT, 1, createTestRequestNFrame()),
            toAbstractionLeakingFrame(DEFAULT, 1, createTestRequestResponseFrame()),
            toAbstractionLeakingFrame(DEFAULT, 1, createTestRequestStreamFrame()),
            toAbstractionLeakingFrame(DEFAULT, 1, createTestResumeFrame()),
            toAbstractionLeakingFrame(DEFAULT, 1, createTestResumeOkFrame()),
            toAbstractionLeakingFrame(DEFAULT, 1, createTestSetupFrame()));

    when(delegate.receive()).thenReturn(frames);

    new MicrometerDuplexConnection(
            CLIENT, delegate, meterRegistry, Tag.of("test-key", "test-value"))
        .receive()
        .as(StepVerifier::create)
        .expectNextCount(14)
        .verifyComplete();

    assertThat(findCounter(CLIENT, CANCEL).count()).isEqualTo(1);
    assertThat(findCounter(CLIENT, COMPLETE).count()).isEqualTo(1);
    assertThat(findCounter(CLIENT, ERROR).count()).isEqualTo(1);
    assertThat(findCounter(CLIENT, KEEPALIVE).count()).isEqualTo(1);
    assertThat(findCounter(CLIENT, LEASE).count()).isEqualTo(1);
    assertThat(findCounter(CLIENT, METADATA_PUSH).count()).isEqualTo(1);
    assertThat(findCounter(CLIENT, REQUEST_CHANNEL).count()).isEqualTo(1);
    assertThat(findCounter(CLIENT, REQUEST_FNF).count()).isEqualTo(1);
    assertThat(findCounter(CLIENT, REQUEST_N).count()).isEqualTo(1);
    assertThat(findCounter(CLIENT, REQUEST_RESPONSE).count()).isEqualTo(1);
    assertThat(findCounter(CLIENT, REQUEST_STREAM).count()).isEqualTo(1);
    assertThat(findCounter(CLIENT, RESUME).count()).isEqualTo(1);
    assertThat(findCounter(CLIENT, RESUME_OK).count()).isEqualTo(1);
    assertThat(findCounter(CLIENT, SETUP).count()).isEqualTo(1);
  }

  @DisplayName("send gathers metrics")
  @SuppressWarnings("unchecked")
  @Test
  void send() {
    ArgumentCaptor<Publisher<Frame>> captor = ArgumentCaptor.forClass(Publisher.class);
    when(delegate.send(captor.capture())).thenReturn(Mono.empty());

    Flux<Frame> frames =
        Flux.just(
            toAbstractionLeakingFrame(DEFAULT, 1, createTestCancelFrame()),
            toAbstractionLeakingFrame(DEFAULT, 1, createTestErrorFrame()),
            toAbstractionLeakingFrame(DEFAULT, 1, createTestKeepaliveFrame()),
            toAbstractionLeakingFrame(DEFAULT, 1, createTestLeaseFrame()),
            toAbstractionLeakingFrame(DEFAULT, 1, createTestMetadataPushFrame()),
            toAbstractionLeakingFrame(DEFAULT, 1, createTestPayloadFrame()),
            toAbstractionLeakingFrame(DEFAULT, 1, createTestRequestChannelFrame()),
            toAbstractionLeakingFrame(DEFAULT, 1, createTestRequestFireAndForgetFrame()),
            toAbstractionLeakingFrame(DEFAULT, 1, createTestRequestNFrame()),
            toAbstractionLeakingFrame(DEFAULT, 1, createTestRequestResponseFrame()),
            toAbstractionLeakingFrame(DEFAULT, 1, createTestRequestStreamFrame()),
            toAbstractionLeakingFrame(DEFAULT, 1, createTestResumeFrame()),
            toAbstractionLeakingFrame(DEFAULT, 1, createTestResumeOkFrame()),
            toAbstractionLeakingFrame(DEFAULT, 1, createTestSetupFrame()));

    new MicrometerDuplexConnection(
            SERVER, delegate, meterRegistry, Tag.of("test-key", "test-value"))
        .send(frames)
        .as(StepVerifier::create)
        .verifyComplete();

    StepVerifier.create(captor.getValue()).expectNextCount(14).verifyComplete();

    assertThat(findCounter(SERVER, CANCEL).count()).isEqualTo(1);
    assertThat(findCounter(SERVER, COMPLETE).count()).isEqualTo(1);
    assertThat(findCounter(SERVER, ERROR).count()).isEqualTo(1);
    assertThat(findCounter(SERVER, KEEPALIVE).count()).isEqualTo(1);
    assertThat(findCounter(SERVER, LEASE).count()).isEqualTo(1);
    assertThat(findCounter(SERVER, METADATA_PUSH).count()).isEqualTo(1);
    assertThat(findCounter(SERVER, REQUEST_CHANNEL).count()).isEqualTo(1);
    assertThat(findCounter(SERVER, REQUEST_FNF).count()).isEqualTo(1);
    assertThat(findCounter(SERVER, REQUEST_N).count()).isEqualTo(1);
    assertThat(findCounter(SERVER, REQUEST_RESPONSE).count()).isEqualTo(1);
    assertThat(findCounter(SERVER, REQUEST_STREAM).count()).isEqualTo(1);
    assertThat(findCounter(SERVER, RESUME).count()).isEqualTo(1);
    assertThat(findCounter(SERVER, RESUME_OK).count()).isEqualTo(1);
    assertThat(findCounter(SERVER, SETUP).count()).isEqualTo(1);
  }

  @DisplayName("send throws NullPointerException with null frames")
  @Test
  void sendNullFrames() {
    assertThatNullPointerException()
        .isThrownBy(
            () -> new MicrometerDuplexConnection(CLIENT, delegate, meterRegistry).send(null))
        .withMessage("frames must not be null");
  }

  private Counter findCounter(Type connectionType, FrameType frameType) {
    return meterRegistry
        .get("rsocket.frame")
        .tag("connection.type", connectionType.name())
        .tag("frame.type", frameType.name())
        .tag("test-key", "test-value")
        .counter();
  }
}
