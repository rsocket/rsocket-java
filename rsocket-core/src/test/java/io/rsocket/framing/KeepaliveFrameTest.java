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
import static io.rsocket.framing.KeepaliveFrame.createKeepaliveFrame;
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

final class KeepaliveFrameTest implements DataFrameTest<KeepaliveFrame> {

  @Override
  public Function<ByteBuf, KeepaliveFrame> getCreateFrameFromByteBuf() {
    return KeepaliveFrame::createKeepaliveFrame;
  }

  @Override
  public Tuple2<KeepaliveFrame, ByteBuf> getFrame() {
    ByteBuf byteBuf =
        Unpooled.buffer(12)
            .writeShort(0b00001100_00000000)
            .writeLong(0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_01100100)
            .writeBytes(getRandomByteBuf(2));

    KeepaliveFrame frame = createKeepaliveFrame(byteBuf);

    return Tuples.of(frame, byteBuf);
  }

  @Override
  public Tuple2<KeepaliveFrame, ByteBuf> getFrameWithData() {
    ByteBuf data = getRandomByteBuf(2);

    KeepaliveFrame frame =
        createKeepaliveFrame(
            Unpooled.buffer(12)
                .writeShort(0b00001100_00000000)
                .writeLong(
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_01100100)
                .writeBytes(data, 0, data.readableBytes()));

    return Tuples.of(frame, data);
  }

  @Override
  public KeepaliveFrame getFrameWithEmptyData() {
    return createKeepaliveFrame(
        Unpooled.buffer(10)
            .writeShort(0b00001100_00000000)
            .writeLong(0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_01100100));
  }

  @DisplayName("creates KEEPALIVE frame with data")
  @Test
  void createKeepAliveFrameData() {
    ByteBuf data = getRandomByteBuf(2);

    ByteBuf expected =
        Unpooled.buffer(12)
            .writeShort(0b00001100_00000000)
            .writeLong(0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_01100100)
            .writeBytes(data, 0, data.readableBytes());

    assertThat(createKeepaliveFrame(DEFAULT, false, 100, data).mapFrame(Function.identity()))
        .isEqualTo(expected);
  }

  @DisplayName("creates KEEPALIVE frame without data")
  @Test
  void createKeepAliveFrameDataNull() {
    ByteBuf expected =
        Unpooled.buffer(10)
            .writeShort(0b00001100_00000000)
            .writeLong(0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_01100100);

    assertThat(createKeepaliveFrame(DEFAULT, false, 100, null).mapFrame(Function.identity()))
        .isEqualTo(expected);
  }

  @DisplayName("creates KEEPALIVE frame without respond flag set")
  @Test
  void createKeepAliveFrameRespondFalse() {
    ByteBuf expected =
        Unpooled.buffer(10)
            .writeShort(0b00001100_00000000)
            .writeLong(0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_01100100);

    assertThat(createKeepaliveFrame(DEFAULT, false, 100, null).mapFrame(Function.identity()))
        .isEqualTo(expected);
  }

  @DisplayName("creates KEEPALIVE frame with respond flag set")
  @Test
  void createKeepAliveFrameRespondTrue() {
    ByteBuf expected =
        Unpooled.buffer(10)
            .writeShort(0b00001100_10000000)
            .writeLong(0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_01100100);

    assertThat(createKeepaliveFrame(DEFAULT, true, 100, null).mapFrame(Function.identity()))
        .isEqualTo(expected);
  }

  @DisplayName("createKeepaliveFrame throws NullPointerException with null byteBufAllocator")
  @Test
  void createKeepaliveFrameNullByteBufAllocator() {
    assertThatNullPointerException()
        .isThrownBy(() -> createKeepaliveFrame(null, true, 100, null))
        .withMessage("byteBufAllocator must not be null");
  }

  @DisplayName("returns last received position")
  @Test
  void getLastReceivedPosition() {
    KeepaliveFrame frame =
        createKeepaliveFrame(
            Unpooled.buffer(10)
                .writeShort(0b00001100_00000000)
                .writeLong(
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_01100100));

    assertThat(frame.getLastReceivedPosition()).isEqualTo(100);
  }

  @DisplayName("tests respond flag not set")
  @Test
  void isRespondFlagSetFalse() {
    KeepaliveFrame frame =
        createKeepaliveFrame(
            Unpooled.buffer(10)
                .writeShort(0b00001100_00000000)
                .writeLong(
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_01100100));

    assertThat(frame.isRespondFlagSet()).isFalse();
  }

  @DisplayName("tests respond flag set")
  @Test
  void isRespondFlagSetTrue() {
    KeepaliveFrame frame =
        createKeepaliveFrame(
            Unpooled.buffer(10)
                .writeShort(0b00001100_10000000)
                .writeLong(
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_01100100));

    assertThat(frame.isRespondFlagSet()).isTrue();
  }
}
