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
import static io.rsocket.framing.ResumeFrame.createResumeFrame;
import static io.rsocket.test.util.ByteBufUtils.getRandomByteBuf;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.function.Function;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

final class ResumeFrameTest implements FrameTest<ResumeFrame> {

  @Override
  public Function<ByteBuf, ResumeFrame> getCreateFrameFromByteBuf() {
    return ResumeFrame::createResumeFrame;
  }

  @Override
  public Tuple2<ResumeFrame, ByteBuf> getFrame() {
    ByteBuf resumeIdentificationToken = getRandomByteBuf(2);

    ByteBuf byteBuf =
        Unpooled.buffer(26)
            .writeShort(0b00110100_00000000)
            .writeShort(0b00000000_01100100)
            .writeShort(0b00000000_11001000)
            .writeShort(0b00000000_00000010)
            .writeBytes(resumeIdentificationToken, 0, resumeIdentificationToken.readableBytes())
            .writeLong(0b00000000_00000000_00000000_00000000_00000000_00000000_00000001_00101100)
            .writeLong(0b00000000_00000000_00000000_00000000_00000000_00000000_00000001_10010000);

    ResumeFrame frame = createResumeFrame(DEFAULT, 100, 200, resumeIdentificationToken, 300, 400);

    return Tuples.of(frame, byteBuf);
  }

  @DisplayName("creates RESUME frame with ByteBufAllocator")
  @Test
  void createResumeByteBufAllocator() {
    ByteBuf resumeIdentificationToken = getRandomByteBuf(2);

    ByteBuf expected =
        Unpooled.buffer(26)
            .writeShort(0b00110100_00000000)
            .writeShort(0b00000000_01100100)
            .writeShort(0b00000000_11001000)
            .writeShort(0b00000000_00000010)
            .writeBytes(resumeIdentificationToken, 0, resumeIdentificationToken.readableBytes())
            .writeLong(0b00000000_00000000_00000000_00000000_00000000_00000000_00000001_00101100)
            .writeLong(0b00000000_00000000_00000000_00000000_00000000_00000000_00000001_10010000);

    assertThat(
            createResumeFrame(DEFAULT, 100, 200, resumeIdentificationToken, 300, 400)
                .mapFrame(Function.identity()))
        .isEqualTo(expected);
  }

  @DisplayName("createResumeFrame throws NullPointerException with null byteBufAllocator")
  @Test
  void createResumeFrameNullByteBufAllocator() {
    assertThatNullPointerException()
        .isThrownBy(() -> createResumeFrame(null, 100, 200, EMPTY_BUFFER, 300, 400))
        .withMessage("byteBufAllocator must not be null");
  }

  @DisplayName("createResumeFrame throws NullPointerException with null resumeIdentificationToken")
  @Test
  void createResumeFrameNullResumeIdentificationToken() {
    assertThatNullPointerException()
        .isThrownBy(() -> createResumeFrame(DEFAULT, 100, 200, null, 300, 400))
        .withMessage("resumeIdentificationToken must not be null");
  }

  @DisplayName("returns first available client position")
  @Test
  void getFirstAvailableClientPosition() {
    ResumeFrame frame =
        createResumeFrame(
            Unpooled.buffer(24)
                .writeShort(0b00110100_00000000)
                .writeShort(0b00000000_01100100)
                .writeShort(0b00000000_11001000)
                .writeShort(0b00000000_00000000)
                .writeBytes(EMPTY_BUFFER)
                .writeLong(
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000001_00101100)
                .writeLong(
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000001_10010000));

    assertThat(frame.getFirstAvailableClientPosition()).isEqualTo(400);
  }

  @DisplayName("returns last received server position")
  @Test
  void getLastReceivedServerPosition() {
    ResumeFrame frame =
        createResumeFrame(
            Unpooled.buffer(24)
                .writeShort(0b00110100_00000000)
                .writeShort(0b00000000_01100100)
                .writeShort(0b00000000_11001000)
                .writeShort(0b00000000_00000000)
                .writeBytes(EMPTY_BUFFER)
                .writeLong(
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000001_00101100)
                .writeLong(
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000001_10010000));

    assertThat(frame.getLastReceivedServerPosition()).isEqualTo(300);
  }

  @DisplayName("returns major version")
  @Test
  void getMajorVersion() {
    ResumeFrame frame =
        createResumeFrame(
            Unpooled.buffer(24)
                .writeShort(0b00110100_00000000)
                .writeShort(0b00000000_01100100)
                .writeShort(0b00000000_11001000)
                .writeShort(0b00000000_00000000)
                .writeBytes(EMPTY_BUFFER)
                .writeLong(
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000001_00101100)
                .writeLong(
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000001_10010000));

    assertThat(frame.getMajorVersion()).isEqualTo(100);
  }

  @DisplayName("returns minor version")
  @Test
  void getMinorVersion() {
    ResumeFrame frame =
        createResumeFrame(
            Unpooled.buffer(24)
                .writeShort(0b00110100_00000000)
                .writeShort(0b00000000_01100100)
                .writeShort(0b00000000_11001000)
                .writeShort(0b00000000_00000000)
                .writeBytes(EMPTY_BUFFER)
                .writeLong(
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000001_00101100)
                .writeLong(
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000001_10010000));

    assertThat(frame.getMinorVersion()).isEqualTo(200);
  }

  @DisplayName("returns resume identification token as UTF-8")
  @Test
  void getResumeIdentificationTokenAsUtf8() {
    ByteBuf resumeIdentificationToken = getRandomByteBuf(2);

    ResumeFrame frame =
        createResumeFrame(
            Unpooled.buffer(26)
                .writeShort(0b00110100_00000000)
                .writeShort(0b00000000_01100100)
                .writeShort(0b00000000_11001000)
                .writeShort(0b00000000_00000010)
                .writeBytes(resumeIdentificationToken, 0, resumeIdentificationToken.readableBytes())
                .writeLong(
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000001_00101100)
                .writeLong(
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000001_10010000));

    assertThat(frame.getResumeIdentificationTokenAsUtf8())
        .isEqualTo(resumeIdentificationToken.toString(UTF_8));
  }

  @DisplayName("returns unsafe resume identification token")
  @Test
  void getUnsafeResumeIdentificationToken() {
    ByteBuf resumeIdentificationToken = getRandomByteBuf(2);

    ResumeFrame frame =
        createResumeFrame(
            Unpooled.buffer(26)
                .writeShort(0b00110100_00000000)
                .writeShort(0b00000000_01100100)
                .writeShort(0b00000000_11001000)
                .writeShort(0b00000000_00000010)
                .writeBytes(resumeIdentificationToken, 0, resumeIdentificationToken.readableBytes())
                .writeLong(
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000001_00101100)
                .writeLong(
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000001_10010000));

    assertThat(frame.getUnsafeResumeIdentificationToken()).isEqualTo(resumeIdentificationToken);
  }

  @DisplayName("maps resume identification token")
  @Test
  void mapResumeIdentificationToken() {
    ByteBuf resumeIdentificationToken = getRandomByteBuf(2);

    ResumeFrame frame =
        createResumeFrame(
            Unpooled.buffer(26)
                .writeShort(0b00110100_00000000)
                .writeShort(0b00000000_01100100)
                .writeShort(0b00000000_11001000)
                .writeShort(0b00000000_00000010)
                .writeBytes(resumeIdentificationToken, 0, resumeIdentificationToken.readableBytes())
                .writeLong(
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000001_00101100)
                .writeLong(
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000001_10010000));

    assertThat(frame.mapResumeIdentificationToken(Function.identity()))
        .isEqualTo(resumeIdentificationToken);
  }

  @DisplayName("mapResumeIdentificationToken throws NullPointerException with null function")
  @Test
  void mapResumeIdentificationTokenNullFunction() {
    ByteBuf resumeIdentificationToken = getRandomByteBuf(2);

    ResumeFrame frame =
        createResumeFrame(
            Unpooled.buffer(26)
                .writeShort(0b00110100_00000000)
                .writeShort(0b00000000_01100100)
                .writeShort(0b00000000_11001000)
                .writeShort(0b00000000_00000010)
                .writeBytes(resumeIdentificationToken, 0, resumeIdentificationToken.readableBytes())
                .writeLong(
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000001_00101100)
                .writeLong(
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000001_10010000));

    assertThatNullPointerException()
        .isThrownBy(() -> frame.mapResumeIdentificationToken(null))
        .withMessage("function must not be null");
  }
}
