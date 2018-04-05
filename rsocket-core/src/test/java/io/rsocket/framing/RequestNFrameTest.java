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
import static io.rsocket.framing.RequestNFrame.createRequestNFrame;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.function.Function;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

final class RequestNFrameTest implements FrameTest<RequestNFrame> {

  @Override
  public Function<ByteBuf, RequestNFrame> getCreateFrameFromByteBuf() {
    return RequestNFrame::createRequestNFrame;
  }

  @Override
  public Tuple2<RequestNFrame, ByteBuf> getFrame() {
    ByteBuf byteBuf =
        Unpooled.buffer(6)
            .writeShort(0b00100000_00000000)
            .writeInt(0b00000000_00000000_00000000_01100100);

    RequestNFrame frame = createRequestNFrame(byteBuf);

    return Tuples.of(frame, byteBuf);
  }

  @DisplayName("creates REQUEST_N frame with ByteBufAllocator")
  @Test
  void createRequestNFrameByteBufAllocator() {
    ByteBuf expected =
        Unpooled.buffer(6)
            .writeShort(0b00100000_00000000)
            .writeInt(0b00000000_00000000_00000000_01100100);

    assertThat(createRequestNFrame(DEFAULT, 100).mapFrame(Function.identity())).isEqualTo(expected);
  }

  @DisplayName("createRequestNFrame throws NullPointerException with null byteBufAllocator")
  @Test
  void createRequestNFrameNullByteBufAllocator() {
    assertThatNullPointerException()
        .isThrownBy(() -> createRequestNFrame(null, 1))
        .withMessage("byteBufAllocator must not be null");
  }

  @DisplayName("createRequestNFrame throws IllegalArgumentException with requestN less then 1")
  @Test
  void createRequestNFrameZeroRequestN() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> createRequestNFrame(DEFAULT, 0))
        .withMessage("requestN must be positive");
  }

  @DisplayName("returns requestN")
  @Test
  void getRequestN() {
    RequestNFrame frame =
        createRequestNFrame(
            Unpooled.buffer(6)
                .writeShort(0b00100000_00000000)
                .writeInt(0b00000000_00000000_00000000_01100100));

    assertThat(frame.getRequestN()).isEqualTo(100);
  }
}
