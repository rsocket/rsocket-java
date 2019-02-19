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
import static io.rsocket.fragmentation.FrameReassembler.createFrameReassembler;
import static io.rsocket.framing.PayloadFrame.createPayloadFrame;
import static io.rsocket.framing.RequestStreamFrame.createRequestStreamFrame;
import static io.rsocket.framing.TestFrames.createTestCancelFrame;
import static io.rsocket.test.util.ByteBufUtils.getRandomByteBuf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.netty.buffer.ByteBuf;
import io.rsocket.framing.CancelFrame;
import io.rsocket.framing.PayloadFrame;
import io.rsocket.framing.RequestStreamFrame;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@Disabled
final class FrameReassemblerTest {

  @DisplayName("createFrameReassembler throws NullPointerException")
  @Test
  void createFrameReassemblerNullByteBufAllocator() {
    assertThatNullPointerException()
        .isThrownBy(() -> createFrameReassembler(null))
        .withMessage("byteBufAllocator must not be null");
  }

  @DisplayName("reassembles data")
  @Test
  void reassembleData() {
    ByteBuf data = getRandomByteBuf(6);

    RequestStreamFrame frame = createRequestStreamFrame(DEFAULT, false, 1, null, data);

    RequestStreamFrame fragment1 =
        createRequestStreamFrame(DEFAULT, true, 1, null, data.slice(0, 2));

    PayloadFrame fragment2 = createPayloadFrame(DEFAULT, true, false, null, data.slice(2, 2));

    PayloadFrame fragment3 = createPayloadFrame(DEFAULT, false, false, null, data.slice(4, 2));

    FrameReassembler frameReassembler = createFrameReassembler(DEFAULT);

    assertThat(frameReassembler.reassemble(fragment1)).isNull();
    assertThat(frameReassembler.reassemble(fragment2)).isNull();
    assertThat(frameReassembler.reassemble(fragment3)).isEqualTo(frame);
  }

  @DisplayName("reassembles metadata")
  @Test
  void reassembleMetadata() {
    ByteBuf metadata = getRandomByteBuf(6);

    RequestStreamFrame frame = createRequestStreamFrame(DEFAULT, false, 1, metadata, null);

    RequestStreamFrame fragment1 =
        createRequestStreamFrame(DEFAULT, true, 1, metadata.slice(0, 2), null);

    PayloadFrame fragment2 = createPayloadFrame(DEFAULT, true, true, metadata.slice(2, 2), null);

    PayloadFrame fragment3 = createPayloadFrame(DEFAULT, false, true, metadata.slice(4, 2), null);

    FrameReassembler frameReassembler = createFrameReassembler(DEFAULT);

    assertThat(frameReassembler.reassemble(fragment1)).isNull();
    assertThat(frameReassembler.reassemble(fragment2)).isNull();
    assertThat(frameReassembler.reassemble(fragment3)).isEqualTo(frame);
  }

  @DisplayName("reassembles metadata and data")
  @Test
  void reassembleMetadataAndData() {
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

    FrameReassembler frameReassembler = createFrameReassembler(DEFAULT);

    assertThat(frameReassembler.reassemble(fragment1)).isNull();
    assertThat(frameReassembler.reassemble(fragment2)).isNull();
    assertThat(frameReassembler.reassemble(fragment3)).isNull();
    assertThat(frameReassembler.reassemble(fragment4)).isNull();
    assertThat(frameReassembler.reassemble(fragment5)).isEqualTo(frame);
  }

  @DisplayName("does not reassemble a non-fragment frame")
  @Test
  void reassembleNonFragment() {
    PayloadFrame frame = createPayloadFrame(DEFAULT, false, true, (ByteBuf) null, null);

    assertThat(createFrameReassembler(DEFAULT).reassemble(frame)).isEqualTo(frame);
  }

  @DisplayName("does not reassemble non fragmentable frame")
  @Test
  void reassembleNonFragmentableFrame() {
    CancelFrame frame = createTestCancelFrame();

    assertThat(createFrameReassembler(DEFAULT).reassemble(frame)).isEqualTo(frame);
  }

  @DisplayName("reassemble throws NullPointerException with null frame")
  @Test
  void reassembleNullFrame() {
    assertThatNullPointerException()
        .isThrownBy(() -> createFrameReassembler(DEFAULT).reassemble(null))
        .withMessage("frame must not be null");
  }
}
