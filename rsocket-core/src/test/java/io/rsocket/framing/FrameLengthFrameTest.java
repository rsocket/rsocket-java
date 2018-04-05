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
import static io.rsocket.framing.FrameLengthFrame.createFrameLengthFrame;
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

final class FrameLengthFrameTest implements FrameTest<FrameLengthFrame> {

  @Override
  public Function<ByteBuf, FrameLengthFrame> getCreateFrameFromByteBuf() {
    return FrameLengthFrame::createFrameLengthFrame;
  }

  @Override
  public Tuple2<FrameLengthFrame, ByteBuf> getFrame() {
    ByteBuf byteBuf =
        Unpooled.buffer(5)
            .writeMedium(0b00000000_00000000_00000010)
            .writeBytes(getRandomByteBuf(2));

    FrameLengthFrame frame = createFrameLengthFrame(byteBuf);

    return Tuples.of(frame, byteBuf);
  }

  @DisplayName("creates frame length frame with ByteBufAllocator")
  @Test
  void createFrameLengthFrameByteBufAllocator() {
    ByteBuf frame = getRandomByteBuf(2);

    ByteBuf expected =
        Unpooled.buffer(5)
            .writeMedium(0b00000000_00000000_00000010)
            .writeBytes(frame, 0, frame.readableBytes());

    assertThat(
            createFrameLengthFrame(DEFAULT, createTestFrame(frame)).mapFrame(Function.identity()))
        .isEqualTo(expected);
  }

  @DisplayName("createFrameLengthFrame throws NullPointerException with null byteBufAllocator")
  @Test
  void createFrameLengthFrameNullByteBufAllocator() {
    assertThatNullPointerException()
        .isThrownBy(() -> createFrameLengthFrame(null, createTestCancelFrame()))
        .withMessage("byteBufAllocator must not be null");
  }

  @DisplayName("returns frame length")
  @Test
  void getFrameLength() {
    FrameLengthFrame frame =
        createFrameLengthFrame(Unpooled.buffer(3).writeMedium(0b00000000_00000000_01100100));

    assertThat(frame.getFrameLength()).isEqualTo(100);
  }

  @DisplayName("maps byteBuf without frame length")
  @Test
  void mapFrameWithoutFrameLength() {
    ByteBuf frame = getRandomByteBuf(2);

    FrameLengthFrame frameLengthFrame =
        createFrameLengthFrame(
            Unpooled.buffer(5)
                .writeMedium(0b00000000_00000000_00000010)
                .writeBytes(frame, 0, frame.readableBytes()));

    assertThat(frameLengthFrame.mapFrameWithoutFrameLength(Function.identity())).isEqualTo(frame);
  }

  @DisplayName("mapFrameWithoutFrameLength throws NullPointerException with null function")
  @Test
  void mapFrameWithoutFrameLengthNullFunction() {
    ByteBuf frame = getRandomByteBuf(2);

    FrameLengthFrame frameLengthFrame =
        createFrameLengthFrame(
            Unpooled.buffer(5)
                .writeMedium(0b00000000_00000000_00000010)
                .writeBytes(frame, 0, frame.readableBytes()));

    assertThatNullPointerException()
        .isThrownBy(() -> frameLengthFrame.mapFrameWithoutFrameLength(null))
        .withMessage("function must not be null");
  }
}
