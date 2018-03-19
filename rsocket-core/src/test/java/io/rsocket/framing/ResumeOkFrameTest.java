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
import static io.rsocket.framing.ResumeOkFrame.createResumeOkFrame;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.function.Function;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

final class ResumeOkFrameTest implements FrameTest<ResumeOkFrame> {

  @Override
  public Function<ByteBuf, ResumeOkFrame> getCreateFrameFromByteBuf() {
    return ResumeOkFrame::createResumeOkFrame;
  }

  @Override
  public Tuple2<ResumeOkFrame, ByteBuf> getFrame() {
    ByteBuf byteBuf =
        Unpooled.buffer(10)
            .writeShort(0b00111000_00000000)
            .writeLong(0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_01100100);

    ResumeOkFrame frame = createResumeOkFrame(byteBuf);

    return Tuples.of(frame, byteBuf);
  }

  @DisplayName("creates RESUME_OK frame with ByteBufAllocator")
  @Test
  void createResumeOkFrameByteBufAllocator() {
    ByteBuf expected =
        Unpooled.buffer(10)
            .writeShort(0b00111000_00000000)
            .writeLong(0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_01100100);

    assertThat(createResumeOkFrame(DEFAULT, 100).mapFrame(Function.identity())).isEqualTo(expected);
  }

  @DisplayName("createResumeOkFrame throws NullPointerException with null byteBufAllocator")
  @Test
  void createResumeOkFrameNullByteBufAllocator() {
    assertThatNullPointerException()
        .isThrownBy(() -> createResumeOkFrame(null, 100))
        .withMessage("byteBufAllocator must not be null");
  }

  @DisplayName("returns last received client position")
  @Test
  void getLastReceivedClientPosition() {
    ResumeOkFrame frame =
        createResumeOkFrame(
            Unpooled.buffer(10)
                .writeShort(0b001110_0000000000)
                .writeLong(
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_01100100));

    assertThat(frame.getLastReceivedClientPosition()).isEqualTo(100);
  }
}
