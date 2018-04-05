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
import static io.rsocket.framing.RequestStreamFrame.createRequestStreamFrame;
import static io.rsocket.test.util.ByteBufUtils.getRandomByteBuf;
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

final class RequestStreamFrameTest implements FragmentableFrameTest<RequestStreamFrame> {

  @Override
  public Function<ByteBuf, RequestStreamFrame> getCreateFrameFromByteBuf() {
    return RequestStreamFrame::createRequestStreamFrame;
  }

  @Override
  public Tuple2<RequestStreamFrame, ByteBuf> getFrame() {
    ByteBuf byteBuf =
        Unpooled.buffer(11)
            .writeShort(0b00011001_00000000)
            .writeInt(0b00000000_00000000_00000000_00000001)
            .writeMedium(0b00000000_00000000_00000010)
            .writeBytes(getRandomByteBuf(2))
            .writeBytes(getRandomByteBuf(2));

    RequestStreamFrame frame = createRequestStreamFrame(byteBuf);

    return Tuples.of(frame, byteBuf);
  }

  @Override
  public Tuple2<RequestStreamFrame, ByteBuf> getFrameWithData() {
    ByteBuf data = getRandomByteBuf(2);

    RequestStreamFrame frame =
        createRequestStreamFrame(
            Unpooled.buffer(9)
                .writeShort(0b00011000_00000000)
                .writeInt(0b00000000_00000000_00000000_00000001)
                .writeMedium(0b00000000_00000000_00000000)
                .writeBytes(data, 0, data.readableBytes()));

    return Tuples.of(frame, data);
  }

  @Override
  public RequestStreamFrame getFrameWithEmptyData() {
    return createRequestStreamFrame(
        Unpooled.buffer(7)
            .writeShort(0b00011000_00000000)
            .writeInt(0b00000000_00000000_00000000_00000001)
            .writeMedium(0b00000000_00000000_00000000));
  }

  @Override
  public RequestStreamFrame getFrameWithEmptyMetadata() {
    return createRequestStreamFrame(
        Unpooled.buffer(7)
            .writeShort(0b00011001_00000000)
            .writeInt(0b00000000_00000000_00000000_00000001)
            .writeMedium(0b00000000_00000000_00000000));
  }

  @Override
  public RequestStreamFrame getFrameWithFollowsFlagSet() {
    return createRequestStreamFrame(
        Unpooled.buffer(7)
            .writeShort(0b00011000_10000000)
            .writeInt(0b00000000_00000000_00000000_00000001)
            .writeMedium(0b00000000_00000000_00000000));
  }

  @Override
  public Tuple2<RequestStreamFrame, ByteBuf> getFrameWithMetadata() {
    ByteBuf metadata = getRandomByteBuf(2);

    RequestStreamFrame frame =
        createRequestStreamFrame(
            Unpooled.buffer(9)
                .writeShort(0b00011001_00000000)
                .writeInt(0b00000000_00000000_00000000_00000001)
                .writeMedium(0b00000000_00000000_00000010)
                .writeBytes(metadata, 0, metadata.readableBytes()));

    return Tuples.of(frame, metadata);
  }

  @Override
  public RequestStreamFrame getFrameWithoutFollowsFlagSet() {
    return createRequestStreamFrame(
        Unpooled.buffer(7)
            .writeShort(0b00011000_00000000)
            .writeInt(0b00000000_00000000_00000000_00000001)
            .writeMedium(0b00000000_00000000_00000000));
  }

  @Override
  public RequestStreamFrame getFrameWithoutMetadata() {
    return createRequestStreamFrame(
        Unpooled.buffer(7)
            .writeShort(0b00001100_10000000)
            .writeInt(0b00000000_00000000_00000000_00000001)
            .writeMedium(0b00000000_00000000_00000000));
  }

  @DisplayName(
      "createRequestStreamFrame throws IllegalArgumentException with invalid initialRequestN")
  @Test
  void createRequestStreamFrameInvalidInitialRequestN() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> createRequestStreamFrame(DEFAULT, false, 0, (ByteBuf) null, null))
        .withMessage("initialRequestN must be positive");
  }

  @DisplayName("createRequestStreamFrame throws NullPointerException with null byteBufAllocator")
  @Test
  void createRequestStreamFrameNullByteBufAllocator() {
    assertThatNullPointerException()
        .isThrownBy(() -> createRequestStreamFrame(null, false, 100, (ByteBuf) null, null))
        .withMessage("byteBufAllocator must not be null");
  }

  @DisplayName("creates REQUEST_STREAM frame with data")
  @Test
  void createRequestStreamFrameWithData() {
    ByteBuf data = getRandomByteBuf(2);

    ByteBuf expected =
        Unpooled.buffer(11)
            .writeShort(0b00011000_00000000)
            .writeInt(0b00000000_00000000_00000000_01100100)
            .writeMedium(0b00000000_00000000_00000000)
            .writeBytes(data, 0, data.readableBytes());

    RequestStreamFrame frame = createRequestStreamFrame(DEFAULT, false, 100, null, data);

    assertThat(frame.mapFrame(Function.identity())).isEqualTo(expected);
  }

  @DisplayName("creates REQUEST_STREAM frame with Follows flag")
  @Test
  void createRequestStreamFrameWithFollows() {
    ByteBuf expected =
        Unpooled.buffer(9)
            .writeShort(0b00011000_10000000)
            .writeInt(0b00000000_00000000_00000000_01100100)
            .writeMedium(0b00000000_00000000_00000000);

    RequestStreamFrame frame = createRequestStreamFrame(DEFAULT, true, 100, (ByteBuf) null, null);

    assertThat(frame.mapFrame(Function.identity())).isEqualTo(expected);
  }

  @DisplayName("creates REQUEST_STREAM frame with metadata")
  @Test
  void createRequestStreamFrameWithMetadata() {
    ByteBuf metadata = getRandomByteBuf(2);

    ByteBuf expected =
        Unpooled.buffer(11)
            .writeShort(0b00011001_00000000)
            .writeInt(0b00000000_00000000_00000000_01100100)
            .writeMedium(0b00000000_00000000_00000010)
            .writeBytes(metadata, 0, metadata.readableBytes());

    RequestStreamFrame frame = createRequestStreamFrame(DEFAULT, false, 100, metadata, null);

    assertThat(frame.mapFrame(Function.identity())).isEqualTo(expected);
  }

  @DisplayName("creates REQUEST_STREAM frame with metadata and data")
  @Test
  void createRequestStreamFrameWithMetadataAnData() {
    ByteBuf metadata = getRandomByteBuf(2);
    ByteBuf data = getRandomByteBuf(2);

    ByteBuf expected =
        Unpooled.buffer(13)
            .writeShort(0b00011001_00000000)
            .writeInt(0b00000000_00000000_00000000_01100100)
            .writeMedium(0b00000000_00000000_00000010)
            .writeBytes(metadata, 0, metadata.readableBytes())
            .writeBytes(data, 0, data.readableBytes());

    RequestStreamFrame frame = createRequestStreamFrame(DEFAULT, false, 100, metadata, data);

    assertThat(frame.mapFrame(Function.identity())).isEqualTo(expected);
  }

  @DisplayName("returns the initial requestN")
  @Test
  void getInitialRequestN() {
    RequestStreamFrame frame =
        createRequestStreamFrame(
            Unpooled.buffer(7)
                .writeShort(0b00011000_10000000)
                .writeInt(0b00000000_00000000_00000000_00000001)
                .writeMedium(0b00000000_00000000_00000000));

    assertThat(frame.getInitialRequestN()).isEqualTo(1);
  }
}
