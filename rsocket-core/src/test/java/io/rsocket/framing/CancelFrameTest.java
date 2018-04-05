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
import static io.rsocket.framing.CancelFrame.createCancelFrame;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.util.function.Function;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

final class CancelFrameTest implements FrameTest<CancelFrame> {

  @Override
  public Function<ByteBuf, CancelFrame> getCreateFrameFromByteBuf() {
    return CancelFrame::createCancelFrame;
  }

  @Override
  public Tuple2<CancelFrame, ByteBuf> getFrame() {
    ByteBuf byteBuf = Unpooled.buffer(2).writeShort(0b00100100_00000000);
    CancelFrame frame = createCancelFrame(byteBuf);

    return Tuples.of(frame, byteBuf);
  }

  @DisplayName("creates CANCEL frame with ByteBufAllocator")
  @Test
  void createCancelFrameByteBufAllocator() {
    ByteBuf expected = Unpooled.buffer(2).writeShort(0b00100100_00000000);

    assertThat(createCancelFrame(DEFAULT).mapFrame(Function.identity())).isEqualTo(expected);
  }

  @DisplayName("createCancelFrame throws NullPointerException with null byteBufAllocator")
  @Test
  void createCancelFrameNullByteBufAllocator() {
    assertThatNullPointerException()
        .isThrownBy(() -> createCancelFrame((ByteBufAllocator) null))
        .withMessage("byteBufAllocator must not be null");
  }
}
