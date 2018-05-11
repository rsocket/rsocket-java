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

package io.rsocket.fragmentation;

import static io.netty.buffer.UnpooledByteBufAllocator.DEFAULT;
import static io.rsocket.framing.PayloadFrame.createPayloadFrame;
import static io.rsocket.framing.RequestStreamFrame.createRequestStreamFrame;
import static io.rsocket.framing.TestFrames.createTestCancelFrame;
import static io.rsocket.test.util.ByteBufUtils.getRandomByteBuf;
import static io.rsocket.util.AbstractionLeakingFrameUtils.toAbstractionLeakingFrame;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

final class FragmentationDuplexConnectionTest {

  private final DuplexConnection delegate = mock(DuplexConnection.class, RETURNS_SMART_NULLS);

  @SuppressWarnings("unchecked")
  private final ArgumentCaptor<Publisher<Frame>> publishers =
      ArgumentCaptor.forClass(Publisher.class);

  @DisplayName("constructor throws IllegalArgumentException with negative maxFragmentLength")
  @Test
  void constructorInvalidMaxFragmentSize() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new FragmentationDuplexConnection(DEFAULT, delegate, Integer.MIN_VALUE))
        .withMessage("maxFragmentSize must be positive");
  }

  @DisplayName("constructor throws NullPointerException with null byteBufAllocator")
  @Test
  void constructorNullByteBufAllocator() {
    assertThatNullPointerException()
        .isThrownBy(() -> new FragmentationDuplexConnection(null, delegate, 2))
        .withMessage("byteBufAllocator must not be null");
  }

  @DisplayName("constructor throws NullPointerException with null delegate")
  @Test
  void constructorNullDelegate() {
    assertThatNullPointerException()
        .isThrownBy(() -> new FragmentationDuplexConnection(DEFAULT, null, 2))
        .withMessage("delegate must not be null");
  }

  @DisplayName("reassembles data")
  @Test
  void reassembleData() {
    ByteBuf data = getRandomByteBuf(6);

    Frame frame =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createRequestStreamFrame(DEFAULT, false, 1, null, data));

    Frame fragment1 =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createRequestStreamFrame(DEFAULT, true, 1, null, data.slice(0, 2)));

    Frame fragment2 =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createPayloadFrame(DEFAULT, true, false, null, data.slice(2, 2)));

    Frame fragment3 =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createPayloadFrame(DEFAULT, false, false, null, data.slice(4, 2)));

    when(delegate.receive()).thenReturn(Flux.just(fragment1, fragment2, fragment3));

    new FragmentationDuplexConnection(DEFAULT, delegate, 2)
        .receive()
        .as(StepVerifier::create)
        .expectNext(frame)
        .verifyComplete();
  }

  @DisplayName("reassembles metadata")
  @Test
  void reassembleMetadata() {
    ByteBuf metadata = getRandomByteBuf(6);

    Frame frame =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createRequestStreamFrame(DEFAULT, false, 1, metadata, null));

    Frame fragment1 =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createRequestStreamFrame(DEFAULT, true, 1, metadata.slice(0, 2), null));

    Frame fragment2 =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createPayloadFrame(DEFAULT, true, true, metadata.slice(2, 2), null));

    Frame fragment3 =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createPayloadFrame(DEFAULT, false, true, metadata.slice(4, 2), null));

    when(delegate.receive()).thenReturn(Flux.just(fragment1, fragment2, fragment3));

    new FragmentationDuplexConnection(DEFAULT, delegate, 2)
        .receive()
        .as(StepVerifier::create)
        .expectNext(frame)
        .verifyComplete();
  }

  @DisplayName("reassembles metadata and data")
  @Test
  void reassembleMetadataAndData() {
    ByteBuf metadata = getRandomByteBuf(5);
    ByteBuf data = getRandomByteBuf(5);

    Frame frame =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createRequestStreamFrame(DEFAULT, false, 1, metadata, data));

    Frame fragment1 =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createRequestStreamFrame(DEFAULT, true, 1, metadata.slice(0, 2), null));

    Frame fragment2 =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createPayloadFrame(DEFAULT, true, true, metadata.slice(2, 2), null));

    Frame fragment3 =
        toAbstractionLeakingFrame(
            DEFAULT,
            1,
            createPayloadFrame(DEFAULT, true, false, metadata.slice(4, 1), data.slice(0, 1)));

    Frame fragment4 =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createPayloadFrame(DEFAULT, true, false, null, data.slice(1, 2)));

    Frame fragment5 =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createPayloadFrame(DEFAULT, false, false, null, data.slice(3, 2)));

    when(delegate.receive())
        .thenReturn(Flux.just(fragment1, fragment2, fragment3, fragment4, fragment5));

    new FragmentationDuplexConnection(DEFAULT, delegate, 2)
        .receive()
        .as(StepVerifier::create)
        .expectNext(frame)
        .verifyComplete();
  }

  @DisplayName("does not reassemble a non-fragment frame")
  @Test
  void reassembleNonFragment() {
    Frame frame =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createPayloadFrame(DEFAULT, false, true, (ByteBuf) null, null));

    when(delegate.receive()).thenReturn(Flux.just(frame));

    new FragmentationDuplexConnection(DEFAULT, delegate, 2)
        .receive()
        .as(StepVerifier::create)
        .expectNext(frame)
        .verifyComplete();
  }

  @DisplayName("does not reassemble non fragmentable frame")
  @Test
  void reassembleNonFragmentableFrame() {
    Frame frame = toAbstractionLeakingFrame(DEFAULT, 1, createTestCancelFrame());

    when(delegate.receive()).thenReturn(Flux.just(frame));

    new FragmentationDuplexConnection(DEFAULT, delegate, 2)
        .receive()
        .as(StepVerifier::create)
        .expectNext(frame)
        .verifyComplete();
  }

  @DisplayName("fragments data")
  @Test
  void sendData() {
    ByteBuf data = getRandomByteBuf(6);

    Frame frame =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createRequestStreamFrame(DEFAULT, false, 1, null, data));

    Frame fragment1 =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createRequestStreamFrame(DEFAULT, true, 1, null, data.slice(0, 2)));

    Frame fragment2 =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createPayloadFrame(DEFAULT, true, false, null, data.slice(2, 2)));

    Frame fragment3 =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createPayloadFrame(DEFAULT, false, false, null, data.slice(4, 2)));

    new FragmentationDuplexConnection(DEFAULT, delegate, 2).sendOne(frame);
    verify(delegate).send(publishers.capture());

    StepVerifier.create(Flux.from(publishers.getValue()))
        .expectNext(fragment1)
        .expectNext(fragment2)
        .expectNext(fragment3)
        .verifyComplete();
  }

  @DisplayName("does not fragment with size equal to maxFragmentLength")
  @Test
  void sendEqualToMaxFragmentLength() {
    Frame frame =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createPayloadFrame(DEFAULT, false, false, null, getRandomByteBuf(2)));

    new FragmentationDuplexConnection(DEFAULT, delegate, 2).sendOne(frame);
    verify(delegate).send(publishers.capture());

    StepVerifier.create(Flux.from(publishers.getValue())).expectNext(frame).verifyComplete();
  }

  @DisplayName("does not fragment an already-fragmented frame")
  @Test
  void sendFragment() {
    Frame frame =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createPayloadFrame(DEFAULT, true, true, (ByteBuf) null, null));

    new FragmentationDuplexConnection(DEFAULT, delegate, 2).sendOne(frame);
    verify(delegate).send(publishers.capture());

    StepVerifier.create(Flux.from(publishers.getValue())).expectNext(frame).verifyComplete();
  }

  @DisplayName("does not fragment with size smaller than maxFragmentLength")
  @Test
  void sendLessThanMaxFragmentLength() {
    Frame frame =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createPayloadFrame(DEFAULT, false, false, null, getRandomByteBuf(1)));

    new FragmentationDuplexConnection(DEFAULT, delegate, 2).sendOne(frame);
    verify(delegate).send(publishers.capture());

    StepVerifier.create(Flux.from(publishers.getValue())).expectNext(frame).verifyComplete();
  }

  @DisplayName("fragments metadata")
  @Test
  void sendMetadata() {
    ByteBuf metadata = getRandomByteBuf(6);

    Frame frame =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createRequestStreamFrame(DEFAULT, false, 1, metadata, null));

    Frame fragment1 =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createRequestStreamFrame(DEFAULT, true, 1, metadata.slice(0, 2), null));

    Frame fragment2 =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createPayloadFrame(DEFAULT, true, true, metadata.slice(2, 2), null));

    Frame fragment3 =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createPayloadFrame(DEFAULT, false, true, metadata.slice(4, 2), null));

    new FragmentationDuplexConnection(DEFAULT, delegate, 2).sendOne(frame);
    verify(delegate).send(publishers.capture());

    StepVerifier.create(Flux.from(publishers.getValue()))
        .expectNext(fragment1)
        .expectNext(fragment2)
        .expectNext(fragment3)
        .verifyComplete();
  }

  @DisplayName("fragments metadata and data")
  @Test
  void sendMetadataAndData() {
    ByteBuf metadata = getRandomByteBuf(5);
    ByteBuf data = getRandomByteBuf(5);

    Frame frame =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createRequestStreamFrame(DEFAULT, false, 1, metadata, data));

    Frame fragment1 =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createRequestStreamFrame(DEFAULT, true, 1, metadata.slice(0, 2), null));

    Frame fragment2 =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createPayloadFrame(DEFAULT, true, true, metadata.slice(2, 2), null));

    Frame fragment3 =
        toAbstractionLeakingFrame(
            DEFAULT,
            1,
            createPayloadFrame(DEFAULT, true, false, metadata.slice(4, 1), data.slice(0, 1)));

    Frame fragment4 =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createPayloadFrame(DEFAULT, true, false, null, data.slice(1, 2)));

    Frame fragment5 =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createPayloadFrame(DEFAULT, false, false, null, data.slice(3, 2)));

    new FragmentationDuplexConnection(DEFAULT, delegate, 2).sendOne(frame);
    verify(delegate).send(publishers.capture());

    StepVerifier.create(Flux.from(publishers.getValue()))
        .expectNext(fragment1)
        .expectNext(fragment2)
        .expectNext(fragment3)
        .expectNext(fragment4)
        .expectNext(fragment5)
        .verifyComplete();
  }

  @DisplayName("does not fragment non-fragmentable frame")
  @Test
  void sendNonFragmentable() {
    Frame frame = toAbstractionLeakingFrame(DEFAULT, 1, createTestCancelFrame());

    new FragmentationDuplexConnection(DEFAULT, delegate, 2).sendOne(frame);
    verify(delegate).send(publishers.capture());

    StepVerifier.create(Flux.from(publishers.getValue())).expectNext(frame).verifyComplete();
  }

  @DisplayName("send throws NullPointerException with null frames")
  @Test
  void sendNullFrames() {
    assertThatNullPointerException()
        .isThrownBy(() -> new FragmentationDuplexConnection(DEFAULT, delegate, 2).send(null))
        .withMessage("frames must not be null");
  }

  @DisplayName("does not fragment with zero maxFragmentLength")
  @Test
  void sendZeroMaxFragmentLength() {
    Frame frame =
        toAbstractionLeakingFrame(
            DEFAULT, 1, createPayloadFrame(DEFAULT, false, false, null, getRandomByteBuf(2)));

    new FragmentationDuplexConnection(DEFAULT, delegate, 0).sendOne(frame);
    verify(delegate).send(publishers.capture());

    StepVerifier.create(Flux.from(publishers.getValue())).expectNext(frame).verifyComplete();
  }
}
