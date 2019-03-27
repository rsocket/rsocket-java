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
import static io.rsocket.framing.RequestChannelFrame.createRequestChannelFrame;
import static io.rsocket.test.util.ByteBufUtils.getRandomByteBuf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.function.Function;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@Disabled
final class RequestChannelFrameTest implements FragmentableFrameTest<RequestChannelFrame> {

  @Override
  public Function<ByteBuf, RequestChannelFrame> getCreateFrameFromByteBuf() {
    return RequestChannelFrame::createRequestChannelFrame;
  }

  @Override
  public Tuple2<RequestChannelFrame, ByteBuf> getFrame() {
    ByteBuf byteBuf =
        Unpooled.buffer(11)
            .writeShort(0b00011101_00000000)
            .writeInt(0b00000000_00000000_00000000_00000001)
            .writeMedium(0b00000000_00000000_00000010)
            .writeBytes(getRandomByteBuf(2))
            .writeBytes(getRandomByteBuf(2));

    RequestChannelFrame frame = createRequestChannelFrame(byteBuf);

    return Tuples.of(frame, byteBuf);
  }

  @Override
  public Tuple2<RequestChannelFrame, ByteBuf> getFrameWithData() {
    ByteBuf data = getRandomByteBuf(2);

    RequestChannelFrame frame =
        createRequestChannelFrame(
            Unpooled.buffer(9)
                .writeShort(0b00011100_00000000)
                .writeInt(0b00000000_00000000_00000000_00000001)
                .writeMedium(0b00000000_00000000_00000000)
                .writeBytes(data, 0, data.readableBytes()));

    return Tuples.of(frame, data);
  }

  @Override
  public RequestChannelFrame getFrameWithEmptyData() {
    return createRequestChannelFrame(
        Unpooled.buffer(7)
            .writeShort(0b00011100_00000000)
            .writeInt(0b00000000_00000000_00000000_00000001)
            .writeMedium(0b00000000_00000000_00000000));
  }

  @Override
  public RequestChannelFrame getFrameWithEmptyMetadata() {
    return createRequestChannelFrame(
        Unpooled.buffer(7)
            .writeShort(0b00011101_00000000)
            .writeInt(0b00000000_00000000_00000000_00000001)
            .writeMedium(0b00000000_00000000_00000000));
  }

  @Override
  public RequestChannelFrame getFrameWithFollowsFlagSet() {
    return createRequestChannelFrame(
        Unpooled.buffer(7)
            .writeShort(0b00011100_10000000)
            .writeInt(0b00000000_00000000_00000000_00000001)
            .writeMedium(0b00000000_00000000_00000000));
  }

  @Override
  public Tuple2<RequestChannelFrame, ByteBuf> getFrameWithMetadata() {
    ByteBuf metadata = getRandomByteBuf(2);

    RequestChannelFrame frame =
        createRequestChannelFrame(
            Unpooled.buffer(9)
                .writeShort(0b00011101_00000000)
                .writeInt(0b00000000_00000000_00000000_00000001)
                .writeMedium(0b00000000_00000000_00000010)
                .writeBytes(metadata, 0, metadata.readableBytes()));

    return Tuples.of(frame, metadata);
  }

  @Override
  public RequestChannelFrame getFrameWithoutFollowsFlagSet() {
    return createRequestChannelFrame(
        Unpooled.buffer(7)
            .writeShort(0b00011100_00000000)
            .writeInt(0b00000000_00000000_00000000_00000001)
            .writeMedium(0b00000000_00000000_00000000));
  }

  @Override
  public RequestChannelFrame getFrameWithoutMetadata() {
    return createRequestChannelFrame(
        Unpooled.buffer(7)
            .writeShort(0b00011100_10000000)
            .writeInt(0b00000000_00000000_00000000_00000001)
            .writeMedium(0b00000000_00000000_00000000));
  }

  @DisplayName("createRequestChannelFrame throws NullPointerException with null byteBufAllocator")
  @Test
  void createRequestChannelFrameNullByteBufAllocator() {
    assertThatNullPointerException()
        .isThrownBy(() -> createRequestChannelFrame(null, false, false, 100, (ByteBuf) null, null))
        .withMessage("byteBufAllocator must not be null");
  }

  @DisplayName("creates REQUEST_CHANNEL frame with Complete flag")
  @Test
  void createRequestChannelFrameWithComplete() {
    ByteBuf expected =
        Unpooled.buffer(9)
            .writeShort(0b00011100_01000000)
            .writeInt(0b00000000_00000000_00000000_01100100)
            .writeMedium(0b00000000_00000000_00000000);

    RequestChannelFrame frame =
        createRequestChannelFrame(DEFAULT, false, true, 100, (ByteBuf) null, null);

    assertThat(frame.mapFrame(Function.identity())).isEqualTo(expected);
  }

  @DisplayName("creates REQUEST_CHANNEL frame with data")
  @Test
  void createRequestChannelFrameWithData() {
    ByteBuf data = getRandomByteBuf(2);

    ByteBuf expected =
        Unpooled.buffer(11)
            .writeShort(0b00011100_00000000)
            .writeInt(0b00000000_00000000_00000000_01100100)
            .writeMedium(0b00000000_00000000_00000000)
            .writeBytes(data, 0, data.readableBytes());

    RequestChannelFrame frame = createRequestChannelFrame(DEFAULT, false, false, 100, null, data);

    assertThat(frame.mapFrame(Function.identity())).isEqualTo(expected);
  }

  @DisplayName("creates REQUEST_CHANNEL frame with Follows flag")
  @Test
  void createRequestChannelFrameWithFollows() {
    ByteBuf expected =
        Unpooled.buffer(9)
            .writeShort(0b00011100_10000000)
            .writeInt(0b00000000_00000000_00000000_01100100)
            .writeMedium(0b00000000_00000000_00000000);

    RequestChannelFrame frame =
        createRequestChannelFrame(DEFAULT, true, false, 100, (ByteBuf) null, null);

    assertThat(frame.mapFrame(Function.identity())).isEqualTo(expected);
  }

  @DisplayName("creates REQUEST_CHANNEL frame with metadata")
  @Test
  void createRequestChannelFrameWithMetadata() {
    ByteBuf metadata = getRandomByteBuf(2);

    ByteBuf expected =
        Unpooled.buffer(11)
            .writeShort(0b00011101_00000000)
            .writeInt(0b00000000_00000000_00000000_01100100)
            .writeMedium(0b00000000_00000000_00000010)
            .writeBytes(metadata, 0, metadata.readableBytes());

    RequestChannelFrame frame =
        createRequestChannelFrame(DEFAULT, false, false, 100, metadata, null);

    assertThat(frame.mapFrame(Function.identity())).isEqualTo(expected);
  }

  @DisplayName("creates REQUEST_CHANNEL frame with metadata and data")
  @Test
  void createRequestChannelFrameWithMetadataAnData() {
    ByteBuf metadata = getRandomByteBuf(2);
    ByteBuf data = getRandomByteBuf(2);

    ByteBuf expected =
        Unpooled.buffer(13)
            .writeShort(0b00011101_00000000)
            .writeInt(0b00000000_00000000_00000000_01100100)
            .writeMedium(0b00000000_00000000_00000010)
            .writeBytes(metadata, 0, metadata.readableBytes())
            .writeBytes(data, 0, data.readableBytes());

    RequestChannelFrame frame =
        createRequestChannelFrame(DEFAULT, false, false, 100, metadata, data);

    assertThat(frame.mapFrame(Function.identity())).isEqualTo(expected);
  }

  @DisplayName("returns the initial requestN")
  @Test
  void getInitialRequestN() {
    RequestChannelFrame frame =
        createRequestChannelFrame(
            Unpooled.buffer(7)
                .writeShort(0b00011100_10000000)
                .writeInt(0b00000000_00000000_00000000_00000001)
                .writeMedium(0b00000000_00000000_00000000));

    assertThat(frame.getInitialRequestN()).isEqualTo(1);
  }

  @DisplayName("tests complete flag not set")
  @Test
  void isCompleteFlagSetFalse() {
    RequestChannelFrame frame =
        createRequestChannelFrame(
            Unpooled.buffer(7)
                .writeShort(0b00011100_10000000)
                .writeInt(0b00000000_00000000_00000000_00000001)
                .writeMedium(0b00000000_00000000_00000000));

    assertThat(frame.isCompleteFlagSet()).isFalse();
  }

  @DisplayName("tests complete flag set")
  @Test
  void isCompleteFlagSetTrue() {
    RequestChannelFrame frame =
        createRequestChannelFrame(
            Unpooled.buffer(7)
                .writeShort(0b00011100_11000000)
                .writeInt(0b00000000_00000000_00000000_00000001)
                .writeMedium(0b00000000_00000000_00000000));

    assertThat(frame.isCompleteFlagSet()).isTrue();
  }
}
