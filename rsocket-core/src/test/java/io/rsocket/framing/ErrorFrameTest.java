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

import static io.netty.buffer.Unpooled.EMPTY_BUFFER;
import static io.netty.buffer.UnpooledByteBufAllocator.DEFAULT;
import static io.rsocket.framing.ErrorFrame.createErrorFrame;
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

final class ErrorFrameTest implements DataFrameTest<ErrorFrame> {

  @Override
  public Function<ByteBuf, ErrorFrame> getCreateFrameFromByteBuf() {
    return ErrorFrame::createErrorFrame;
  }

  @Override
  public Tuple2<ErrorFrame, ByteBuf> getFrame() {
    ByteBuf byteBuf =
        Unpooled.buffer(8)
            .writeShort(0b00101100_00000000)
            .writeInt(0b00000000_00000000_00000000_01100100)
            .writeBytes(getRandomByteBuf(2));

    ErrorFrame frame = createErrorFrame(byteBuf);

    return Tuples.of(frame, byteBuf);
  }

  @Override
  public Tuple2<ErrorFrame, ByteBuf> getFrameWithData() {
    ByteBuf data = getRandomByteBuf(2);

    ErrorFrame frame =
        createErrorFrame(
            Unpooled.buffer(8)
                .writeShort(0b00101100_00000000)
                .writeInt(0b00000000_00000000_00000000_01100100)
                .writeBytes(data, 0, data.readableBytes()));

    return Tuples.of(frame, data);
  }

  @Override
  public ErrorFrame getFrameWithEmptyData() {
    return createErrorFrame(
        Unpooled.buffer(6)
            .writeShort(0b00101100_00000000)
            .writeInt(0b00000000_00000000_00000000_01100100));
  }

  @DisplayName("creates KEEPALIVE frame with data")
  @Test
  void createErrorFrameData() {
    ByteBuf data = getRandomByteBuf(2);

    ByteBuf expected =
        Unpooled.buffer(8)
            .writeShort(0b00101100_00000000)
            .writeInt(0b00000000_00000000_00000000_01100100)
            .writeBytes(data, 0, data.readableBytes());

    assertThat(createErrorFrame(DEFAULT, 100, data).mapFrame(Function.identity()))
        .isEqualTo(expected);
  }

  @DisplayName("creates KEEPALIVE frame without data")
  @Test
  void createErrorFrameDataNull() {
    ByteBuf expected =
        Unpooled.buffer(6)
            .writeShort(0b00101100_00000000)
            .writeInt(0b00000000_00000000_00000000_01100100);

    assertThat(createErrorFrame(DEFAULT, 100, (ByteBuf) null).mapFrame(Function.identity()))
        .isEqualTo(expected);
  }

  @DisplayName("createErrorFrame throws NullPointerException with null byteBufAllocator")
  @Test
  void createErrorFrameNullByteBufAllocator() {
    assertThatNullPointerException()
        .isThrownBy(() -> createErrorFrame(null, 0, EMPTY_BUFFER))
        .withMessage("byteBufAllocator must not be null");
  }

  @DisplayName("returns error code")
  @Test
  void getErrorCode() {
    ErrorFrame frame =
        createErrorFrame(
            Unpooled.buffer(6)
                .writeShort(0b00001100_00000000)
                .writeInt(0b00000000_00000000_00000000_01100100));

    assertThat(frame.getErrorCode()).isEqualTo(100);
  }
}
