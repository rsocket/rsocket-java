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
import static io.rsocket.framing.RequestFireAndForgetFrame.createRequestFireAndForgetFrame;
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
final class RequestFireAndForgetFrameTest
    implements FragmentableFrameTest<RequestFireAndForgetFrame> {

  @Override
  public Function<ByteBuf, RequestFireAndForgetFrame> getCreateFrameFromByteBuf() {
    return RequestFireAndForgetFrame::createRequestFireAndForgetFrame;
  }

  @Override
  public Tuple2<RequestFireAndForgetFrame, ByteBuf> getFrame() {
    ByteBuf byteBuf =
        Unpooled.buffer(9)
            .writeShort(0b00010101_00000000)
            .writeMedium(0b00000000_00000000_00000010)
            .writeBytes(getRandomByteBuf(2))
            .writeBytes(getRandomByteBuf(2));

    RequestFireAndForgetFrame frame = createRequestFireAndForgetFrame(byteBuf);

    return Tuples.of(frame, byteBuf);
  }

  @Override
  public Tuple2<RequestFireAndForgetFrame, ByteBuf> getFrameWithData() {
    ByteBuf data = getRandomByteBuf(2);

    RequestFireAndForgetFrame frame =
        createRequestFireAndForgetFrame(
            Unpooled.buffer(7)
                .writeShort(0b00010100_00000000)
                .writeMedium(0b00000000_00000000_00000000)
                .writeBytes(data, 0, data.readableBytes()));

    return Tuples.of(frame, data);
  }

  @Override
  public RequestFireAndForgetFrame getFrameWithEmptyData() {
    return createRequestFireAndForgetFrame(
        Unpooled.buffer(5)
            .writeShort(0b00010100_00000000)
            .writeMedium(0b00000000_00000000_00000000));
  }

  @Override
  public RequestFireAndForgetFrame getFrameWithEmptyMetadata() {
    return createRequestFireAndForgetFrame(
        Unpooled.buffer(5)
            .writeShort(0b00010101_00000000)
            .writeMedium(0b00000000_00000000_00000000));
  }

  @Override
  public RequestFireAndForgetFrame getFrameWithFollowsFlagSet() {
    return createRequestFireAndForgetFrame(
        Unpooled.buffer(5)
            .writeShort(0b00010100_10000000)
            .writeMedium(0b00000000_00000000_00000000));
  }

  @Override
  public Tuple2<RequestFireAndForgetFrame, ByteBuf> getFrameWithMetadata() {
    ByteBuf metadata = getRandomByteBuf(2);

    RequestFireAndForgetFrame frame =
        createRequestFireAndForgetFrame(
            Unpooled.buffer(7)
                .writeShort(0b00010101_00000000)
                .writeMedium(0b00000000_00000000_00000010)
                .writeBytes(metadata, 0, metadata.readableBytes()));

    return Tuples.of(frame, metadata);
  }

  @Override
  public RequestFireAndForgetFrame getFrameWithoutFollowsFlagSet() {
    return createRequestFireAndForgetFrame(
        Unpooled.buffer(5)
            .writeShort(0b00010100_00000000)
            .writeMedium(0b00000000_00000000_00000000));
  }

  @Override
  public RequestFireAndForgetFrame getFrameWithoutMetadata() {
    return createRequestFireAndForgetFrame(
        Unpooled.buffer(5)
            .writeShort(0b00010100_10000000)
            .writeMedium(0b00000000_00000000_00000000));
  }

  @DisplayName(
      "createRequestFireAndForgetFrame throws NullPointerException with null byteBufAllocator")
  @Test
  void createRequestFireAndForgetFrameNullByteBufAllocator() {
    assertThatNullPointerException()
        .isThrownBy(() -> createRequestFireAndForgetFrame(null, false, (ByteBuf) null, null))
        .withMessage("byteBufAllocator must not be null");
  }

  @DisplayName("creates REQUEST_FNF frame with data")
  @Test
  void createRequestFireAndForgetFrameWithData() {
    ByteBuf data = getRandomByteBuf(2);

    ByteBuf expected =
        Unpooled.buffer(7)
            .writeShort(0b00010100_00000000)
            .writeMedium(0b00000000_00000000_00000000)
            .writeBytes(data, 0, data.readableBytes());

    RequestFireAndForgetFrame frame = createRequestFireAndForgetFrame(DEFAULT, false, null, data);

    assertThat(frame.mapFrame(Function.identity())).isEqualTo(expected);
  }

  @DisplayName("creates REQUEST_FNF frame with Follows flag")
  @Test
  void createRequestFireAndForgetFrameWithFollows() {
    ByteBuf expected =
        Unpooled.buffer(5)
            .writeShort(0b00010100_10000000)
            .writeMedium(0b00000000_00000000_00000000);

    RequestFireAndForgetFrame frame =
        createRequestFireAndForgetFrame(DEFAULT, true, (ByteBuf) null, null);

    assertThat(frame.mapFrame(Function.identity())).isEqualTo(expected);
  }

  @DisplayName("creates REQUEST_FNF frame with metadata")
  @Test
  void createRequestFireAndForgetFrameWithMetadata() {
    ByteBuf metadata = getRandomByteBuf(2);

    ByteBuf expected =
        Unpooled.buffer(7)
            .writeShort(0b00010101_00000000)
            .writeMedium(0b00000000_00000000_00000010)
            .writeBytes(metadata, 0, metadata.readableBytes());

    RequestFireAndForgetFrame frame =
        createRequestFireAndForgetFrame(DEFAULT, false, metadata, null);

    assertThat(frame.mapFrame(Function.identity())).isEqualTo(expected);
  }

  @DisplayName("creates REQUEST_FNF frame with metadata and data")
  @Test
  void createRequestFireAndForgetFrameWithMetadataAnData() {
    ByteBuf metadata = getRandomByteBuf(2);
    ByteBuf data = getRandomByteBuf(2);

    ByteBuf expected =
        Unpooled.buffer(9)
            .writeShort(0b00010101_00000000)
            .writeMedium(0b00000000_00000000_00000010)
            .writeBytes(metadata, 0, metadata.readableBytes())
            .writeBytes(data, 0, data.readableBytes());

    RequestFireAndForgetFrame frame =
        createRequestFireAndForgetFrame(DEFAULT, false, metadata, data);

    assertThat(frame.mapFrame(Function.identity())).isEqualTo(expected);
  }
}
