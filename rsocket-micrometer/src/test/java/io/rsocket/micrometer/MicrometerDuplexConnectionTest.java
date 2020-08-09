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

import static io.rsocket.frame.FrameType.*;
import static io.rsocket.plugins.DuplexConnectionInterceptor.Type.CLIENT;
import static io.rsocket.plugins.DuplexConnectionInterceptor.Type.SERVER;
import static io.rsocket.test.TestFrames.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.Mockito.*;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.buffer.ByteBuf;
import io.rsocket.DuplexConnection;
import io.rsocket.frame.FrameType;
import io.rsocket.plugins.DuplexConnectionInterceptor.Type;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.test.StepVerifier;

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
    Flux<ByteBuf> frames =
        Flux.just(
            createTestCancelFrame(),
            createTestErrorFrame(),
            createTestKeepaliveFrame(),
            createTestLeaseFrame(),
            createTestMetadataPushFrame(),
            createTestPayloadFrame(),
            createTestRequestChannelFrame(),
            createTestRequestFireAndForgetFrame(),
            createTestRequestNFrame(),
            createTestRequestResponseFrame(),
            createTestRequestStreamFrame(),
            createTestSetupFrame());

    when(delegate.receive()).thenReturn(frames);

    new MicrometerDuplexConnection(
            CLIENT, delegate, meterRegistry, Tag.of("test-key", "test-value"))
        .receive()
        .as(StepVerifier::create)
        .expectNextCount(12)
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
    assertThat(findCounter(CLIENT, SETUP).count()).isEqualTo(1);
  }

  @DisplayName("send gathers metrics")
  @SuppressWarnings("unchecked")
  @Test
  void send() {
    ArgumentCaptor<ByteBuf> captor = ArgumentCaptor.forClass(ByteBuf.class);
    doNothing().when(delegate).sendFrame(Mockito.anyInt(), captor.capture(), Mockito.anyBoolean());

    final MicrometerDuplexConnection micrometerDuplexConnection =
        new MicrometerDuplexConnection(
            SERVER, delegate, meterRegistry, Tag.of("test-key", "test-value"));
    micrometerDuplexConnection.sendFrame(1, createTestCancelFrame(), false);
    micrometerDuplexConnection.sendFrame(1, createTestErrorFrame(), false);
    micrometerDuplexConnection.sendFrame(1, createTestKeepaliveFrame(), false);
    micrometerDuplexConnection.sendFrame(1, createTestLeaseFrame(), false);
    micrometerDuplexConnection.sendFrame(1, createTestMetadataPushFrame(), false);
    micrometerDuplexConnection.sendFrame(1, createTestPayloadFrame(), false);
    micrometerDuplexConnection.sendFrame(1, createTestRequestChannelFrame(), false);
    micrometerDuplexConnection.sendFrame(1, createTestRequestFireAndForgetFrame(), false);
    micrometerDuplexConnection.sendFrame(1, createTestRequestNFrame(), false);
    micrometerDuplexConnection.sendFrame(1, createTestRequestResponseFrame(), false);
    micrometerDuplexConnection.sendFrame(1, createTestRequestStreamFrame(), false);
    micrometerDuplexConnection.sendFrame(1, createTestSetupFrame(), false);

    StepVerifier.create(Flux.fromIterable(captor.getAllValues()))
        .expectNextCount(12)
        .verifyComplete();

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
    assertThat(findCounter(SERVER, SETUP).count()).isEqualTo(1);
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
