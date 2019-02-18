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
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.netty.buffer.ByteBuf;
import io.rsocket.framing.CancelFrame;
import io.rsocket.framing.PayloadFrame;
import io.rsocket.framing.RequestStreamFrame;
import org.junit.Ignore;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

@Disabled
final class FrameFragmenterTest {

  @DisplayName("constructor throws NullPointerException with null ByteBufAllocator")
  @Test
  void constructorNullByteBufAllocator() {
    assertThatNullPointerException()
        .isThrownBy(() -> new FrameFragmenter(null, 2))
        .withMessage("byteBufAllocator must not be null");
  }

  @DisplayName("fragments data")
  @Test
  void fragmentData() {
    ByteBuf data = getRandomByteBuf(6);

    RequestStreamFrame frame = createRequestStreamFrame(DEFAULT, false, 1, null, data);

    RequestStreamFrame fragment1 =
        createRequestStreamFrame(DEFAULT, true, 1, null, data.slice(0, 2));

    PayloadFrame fragment2 = createPayloadFrame(DEFAULT, true, false, null, data.slice(2, 2));

    PayloadFrame fragment3 = createPayloadFrame(DEFAULT, false, false, null, data.slice(4, 2));

    new FrameFragmenter(DEFAULT, 2)
        .fragment(frame)
        .as(StepVerifier::create)
        .expectNext(fragment1)
        .expectNext(fragment2)
        .expectNext(fragment3)
        .verifyComplete();
  }

  @DisplayName("does not fragment with size equal to maxFragmentLength")
  @Test
  void fragmentEqualToMaxFragmentLength() {
    PayloadFrame frame = createPayloadFrame(DEFAULT, false, false, null, getRandomByteBuf(2));

    new FrameFragmenter(DEFAULT, 2)
        .fragment(frame)
        .as(StepVerifier::create)
        .expectNext(frame)
        .verifyComplete();
  }

  @DisplayName("does not fragment an already-fragmented frame")
  @Test
  void fragmentFragment() {
    PayloadFrame frame = createPayloadFrame(DEFAULT, true, true, (ByteBuf) null, null);

    new FrameFragmenter(DEFAULT, 2)
        .fragment(frame)
        .as(StepVerifier::create)
        .expectNext(frame)
        .verifyComplete();
  }

  @DisplayName("does not fragment with size smaller than maxFragmentLength")
  @Test
  void fragmentLessThanMaxFragmentLength() {
    PayloadFrame frame = createPayloadFrame(DEFAULT, false, false, null, getRandomByteBuf(1));

    new FrameFragmenter(DEFAULT, 2)
        .fragment(frame)
        .as(StepVerifier::create)
        .expectNext(frame)
        .verifyComplete();
  }

  @DisplayName("fragments metadata")
  @Test
  void fragmentMetadata() {
    ByteBuf metadata = getRandomByteBuf(6);

    RequestStreamFrame frame = createRequestStreamFrame(DEFAULT, false, 1, metadata, null);

    RequestStreamFrame fragment1 =
        createRequestStreamFrame(DEFAULT, true, 1, metadata.slice(0, 2), null);

    PayloadFrame fragment2 = createPayloadFrame(DEFAULT, true, true, metadata.slice(2, 2), null);

    PayloadFrame fragment3 = createPayloadFrame(DEFAULT, false, true, metadata.slice(4, 2), null);

    new FrameFragmenter(DEFAULT, 2)
        .fragment(frame)
        .as(StepVerifier::create)
        .expectNext(fragment1)
        .expectNext(fragment2)
        .expectNext(fragment3)
        .verifyComplete();
  }

  @DisplayName("fragments metadata and data")
  @Test
  void fragmentMetadataAndData() {
    ByteBuf metadata = getRandomByteBuf(5);
    ByteBuf data = getRandomByteBuf(5);

    RequestStreamFrame frame = createRequestStreamFrame(DEFAULT, false, 1, metadata, data);

    RequestStreamFrame fragment1 =
        createRequestStreamFrame(DEFAULT, true, 1, metadata.slice(0, 2), null);

    PayloadFrame fragment2 = createPayloadFrame(DEFAULT, true, true, metadata.slice(2, 2), null);

    PayloadFrame fragment3 =
        createPayloadFrame(DEFAULT, true, false, metadata.slice(4, 1), data.slice(0, 1));

    PayloadFrame fragment4 = createPayloadFrame(DEFAULT, true, false, null, data.slice(1, 2));

    PayloadFrame fragment5 = createPayloadFrame(DEFAULT, false, false, null, data.slice(3, 2));

    new FrameFragmenter(DEFAULT, 2)
        .fragment(frame)
        .as(StepVerifier::create)
        .expectNext(fragment1)
        .expectNext(fragment2)
        .expectNext(fragment3)
        .expectNext(fragment4)
        .expectNext(fragment5)
        .verifyComplete();
  }

  @DisplayName("does not fragment non-fragmentable frame")
  @Test
  void fragmentNonFragmentable() {
    CancelFrame frame = createTestCancelFrame();

    new FrameFragmenter(DEFAULT, 2)
        .fragment(frame)
        .as(StepVerifier::create)
        .expectNext(frame)
        .verifyComplete();
  }

  @DisplayName("fragment throws NullPointerException with null frame")
  @Test
  void fragmentWithNullFrame() {
    assertThatNullPointerException()
        .isThrownBy(() -> new FrameFragmenter(DEFAULT, 2).fragment(null))
        .withMessage("frame must not be null");
  }

  @DisplayName("does not fragment with zero maxFragmentLength")
  @Test
  void fragmentZeroMaxFragmentLength() {
    PayloadFrame frame = createPayloadFrame(DEFAULT, false, false, null, getRandomByteBuf(2));

    new FrameFragmenter(DEFAULT, 0)
        .fragment(frame)
        .as(StepVerifier::create)
        .expectNext(frame)
        .verifyComplete();
  }
}
