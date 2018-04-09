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

package io.rsocket.framing;

import static io.netty.buffer.UnpooledByteBufAllocator.DEFAULT;
import static io.rsocket.framing.FrameType.CANCEL;
import static io.rsocket.framing.StreamIdFrame.createStreamIdFrame;
import static io.rsocket.framing.TestFrames.createTestCancelFrame;
import static io.rsocket.framing.TestFrames.createTestFrame;
import static io.rsocket.test.util.ByteBufUtils.getRandomByteBuf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.function.Function;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

final class StreamIdFrameTest implements FrameTest<StreamIdFrame> {

  @Override
  public Function<ByteBuf, StreamIdFrame> getCreateFrameFromByteBuf() {
    return StreamIdFrame::createStreamIdFrame;
  }

  @Override
  public Tuple2<StreamIdFrame, ByteBuf> getFrame() {
    ByteBuf byteBuf =
        Unpooled.buffer(6)
            .writeInt(0b00000000_00000000_00000000_01100100)
            .writeBytes(getRandomByteBuf(2));

    StreamIdFrame frame = createStreamIdFrame(byteBuf);

    return Tuples.of(frame, byteBuf);
  }

  @DisplayName("creates stream id frame with ByteBufAllocator")
  @Test
  void createStreamIdFrameByteBufAllocator() {
    ByteBuf frame = getRandomByteBuf(2);

    ByteBuf expected =
        Unpooled.buffer(6)
            .writeInt(0b00000000_00000000_00000000_01100100)
            .writeBytes(frame, 0, frame.readableBytes());

    assertThat(
            createStreamIdFrame(DEFAULT, 100, createTestFrame(CANCEL, frame))
                .mapFrame(Function.identity()))
        .isEqualTo(expected);
  }

  @DisplayName("createStreamIdFrame throws NullPointerException with null byteBufAllocator")
  @Test
  void createStreamIdFrameNullByteBufAllocator() {
    assertThatNullPointerException()
        .isThrownBy(() -> createStreamIdFrame(null, 0, createTestCancelFrame()))
        .withMessage("byteBufAllocator must not be null");
  }

  @DisplayName("createStreamIdFrame throws NullPointerException with null frame")
  @Test
  void createStreamIdFrameNullFrame() {
    assertThatNullPointerException()
        .isThrownBy(() -> createStreamIdFrame(DEFAULT, 0, null))
        .withMessage("frame must not be null");
  }

  @DisplayName("returns stream id")
  @Test
  void getStreamId() {
    StreamIdFrame frame =
        createStreamIdFrame(Unpooled.buffer(4).writeInt(0b00000000_00000000_00000000_01100100));

    assertThat(frame.getStreamId()).isEqualTo(100);
  }

  @DisplayName("maps byteBuf without stream id")
  @Test
  void mapFrameWithoutStreamId() {
    ByteBuf frame = getRandomByteBuf(2);

    StreamIdFrame streamIdFrame =
        createStreamIdFrame(
            Unpooled.buffer(6)
                .writeInt(0b00000000_00000000_00000000_01100100)
                .writeBytes(frame, 0, frame.readableBytes()));

    assertThat(streamIdFrame.mapFrameWithoutStreamId(Function.identity())).isEqualTo(frame);
  }

  @DisplayName("mapFrameWithoutStreamId throws NullPointerException with null function")
  @Test
  void mapFrameWithoutStreamIdNullFunction() {
    ByteBuf frame = getRandomByteBuf(2);

    StreamIdFrame streamIdFrame =
        createStreamIdFrame(
            Unpooled.buffer(6)
                .writeInt(0b00000000_00000000_00000000_01100100)
                .writeBytes(frame, 0, frame.readableBytes()));

    assertThatNullPointerException()
        .isThrownBy(() -> streamIdFrame.mapFrameWithoutStreamId(null))
        .withMessage("function must not be null");
  }
}
