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
import static io.rsocket.framing.RequestResponseFrame.createRequestResponseFrame;
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

final class RequestResponseFrameTest implements FragmentableFrameTest<RequestResponseFrame> {

  @Override
  public Function<ByteBuf, RequestResponseFrame> getCreateFrameFromByteBuf() {
    return RequestResponseFrame::createRequestResponseFrame;
  }

  @Override
  public Tuple2<RequestResponseFrame, ByteBuf> getFrame() {
    ByteBuf byteBuf =
        Unpooled.buffer(9)
            .writeShort(0b00010001_00000000)
            .writeMedium(0b00000000_00000000_00000010)
            .writeBytes(getRandomByteBuf(2))
            .writeBytes(getRandomByteBuf(2));

    RequestResponseFrame frame = createRequestResponseFrame(byteBuf);

    return Tuples.of(frame, byteBuf);
  }

  @Override
  public Tuple2<RequestResponseFrame, ByteBuf> getFrameWithData() {
    ByteBuf data = getRandomByteBuf(2);

    RequestResponseFrame frame =
        createRequestResponseFrame(
            Unpooled.buffer(7)
                .writeShort(0b00010000_00000000)
                .writeMedium(0b00000000_00000000_00000000)
                .writeBytes(data, 0, data.readableBytes()));

    return Tuples.of(frame, data);
  }

  @Override
  public RequestResponseFrame getFrameWithEmptyData() {
    return createRequestResponseFrame(
        Unpooled.buffer(5)
            .writeShort(0b00010000_00000000)
            .writeMedium(0b00000000_00000000_00000000));
  }

  @Override
  public RequestResponseFrame getFrameWithEmptyMetadata() {
    return createRequestResponseFrame(
        Unpooled.buffer(5)
            .writeShort(0b00010001_00000000)
            .writeMedium(0b00000000_00000000_00000000));
  }

  @Override
  public RequestResponseFrame getFrameWithFollowsFlagSet() {
    return createRequestResponseFrame(
        Unpooled.buffer(5)
            .writeShort(0b00010000_10000000)
            .writeMedium(0b00000000_00000000_00000000));
  }

  @Override
  public Tuple2<RequestResponseFrame, ByteBuf> getFrameWithMetadata() {
    ByteBuf metadata = getRandomByteBuf(2);

    RequestResponseFrame frame =
        createRequestResponseFrame(
            Unpooled.buffer(7)
                .writeShort(0b00010001_00000000)
                .writeMedium(0b00000000_00000000_00000010)
                .writeBytes(metadata, 0, metadata.readableBytes()));

    return Tuples.of(frame, metadata);
  }

  @Override
  public RequestResponseFrame getFrameWithoutFollowsFlagSet() {
    return createRequestResponseFrame(
        Unpooled.buffer(5)
            .writeShort(0b00010000_00000000)
            .writeMedium(0b00000000_00000000_00000000));
  }

  @Override
  public RequestResponseFrame getFrameWithoutMetadata() {
    return createRequestResponseFrame(
        Unpooled.buffer(5)
            .writeShort(0b00010000_10000000)
            .writeMedium(0b00000000_00000000_00000000));
  }

  @DisplayName("createRequestResponseFrame throws NullPointerException with null byteBufAllocator")
  @Test
  void createRequestResponseFrameNullByteBufAllocator() {
    assertThatNullPointerException()
        .isThrownBy(() -> createRequestResponseFrame(null, false, (ByteBuf) null, null))
        .withMessage("byteBufAllocator must not be null");
  }

  @DisplayName("creates REQUEST_FNF frame with data")
  @Test
  void createRequestResponseFrameWithData() {
    ByteBuf data = getRandomByteBuf(2);

    ByteBuf expected =
        Unpooled.buffer(7)
            .writeShort(0b00010000_00000000)
            .writeMedium(0b00000000_00000000_00000000)
            .writeBytes(data, 0, data.readableBytes());

    RequestResponseFrame frame = createRequestResponseFrame(DEFAULT, false, null, data);

    assertThat(frame.mapFrame(Function.identity())).isEqualTo(expected);
  }

  @DisplayName("creates REQUEST_FNF frame with Follows flag")
  @Test
  void createRequestResponseFrameWithFollows() {
    ByteBuf expected =
        Unpooled.buffer(5)
            .writeShort(0b00010000_10000000)
            .writeMedium(0b00000000_00000000_00000000);

    RequestResponseFrame frame = createRequestResponseFrame(DEFAULT, true, (ByteBuf) null, null);

    assertThat(frame.mapFrame(Function.identity())).isEqualTo(expected);
  }

  @DisplayName("creates REQUEST_FNF frame with metadata")
  @Test
  void createRequestResponseFrameWithMetadata() {
    ByteBuf metadata = getRandomByteBuf(2);

    ByteBuf expected =
        Unpooled.buffer(7)
            .writeShort(0b00010001_00000000)
            .writeMedium(0b00000000_00000000_00000010)
            .writeBytes(metadata, 0, metadata.readableBytes());

    RequestResponseFrame frame = createRequestResponseFrame(DEFAULT, false, metadata, null);

    assertThat(frame.mapFrame(Function.identity())).isEqualTo(expected);
  }

  @DisplayName("creates REQUEST_FNF frame with metadata and data")
  @Test
  void createRequestResponseFrameWithMetadataAnData() {
    ByteBuf metadata = getRandomByteBuf(2);
    ByteBuf data = getRandomByteBuf(2);

    ByteBuf expected =
        Unpooled.buffer(9)
            .writeShort(0b00010001_00000000)
            .writeMedium(0b00000000_00000000_00000010)
            .writeBytes(metadata, 0, metadata.readableBytes())
            .writeBytes(data, 0, data.readableBytes());

    RequestResponseFrame frame = createRequestResponseFrame(DEFAULT, false, metadata, data);

    assertThat(frame.mapFrame(Function.identity())).isEqualTo(expected);
  }
}
