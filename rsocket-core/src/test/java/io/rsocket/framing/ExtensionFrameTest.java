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
import static io.rsocket.framing.ExtensionFrame.createExtensionFrame;
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

final class ExtensionFrameTest implements MetadataAndDataFrameTest<ExtensionFrame> {

  @Override
  public Function<ByteBuf, ExtensionFrame> getCreateFrameFromByteBuf() {
    return ExtensionFrame::createExtensionFrame;
  }

  @Override
  public Tuple2<ExtensionFrame, ByteBuf> getFrame() {
    ByteBuf byteBuf =
        Unpooled.buffer(13)
            .writeShort(0b11111101_00000000)
            .writeInt(0b00000000_00000000_00000000_01100100)
            .writeMedium(0b00000000_00000000_00000010)
            .writeBytes(getRandomByteBuf(2))
            .writeBytes(getRandomByteBuf(2));

    ExtensionFrame frame = createExtensionFrame(byteBuf);

    return Tuples.of(frame, byteBuf);
  }

  @Override
  public Tuple2<ExtensionFrame, ByteBuf> getFrameWithData() {
    ByteBuf metadata = getRandomByteBuf(2);
    ByteBuf data = getRandomByteBuf(2);

    ExtensionFrame frame =
        createExtensionFrame(
            Unpooled.buffer(13)
                .writeShort(0b11111101_00000000)
                .writeInt(0b00000000_00000000_00000000_01100100)
                .writeMedium(0b00000000_00000000_00000010)
                .writeBytes(metadata, 0, metadata.readableBytes())
                .writeBytes(data, 0, data.readableBytes()));

    return Tuples.of(frame, data);
  }

  @Override
  public ExtensionFrame getFrameWithEmptyData() {
    ByteBuf metadata = getRandomByteBuf(2);

    return createExtensionFrame(
        Unpooled.buffer(11)
            .writeShort(0b11111101_00000000)
            .writeInt(0b00000000_00000000_00000000_01100100)
            .writeMedium(0b00000000_00000000_00000010)
            .writeBytes(metadata, 0, metadata.readableBytes()));
  }

  @Override
  public ExtensionFrame getFrameWithEmptyMetadata() {
    ByteBuf data = getRandomByteBuf(2);

    return createExtensionFrame(
        Unpooled.buffer(11)
            .writeShort(0b11111101_00000000)
            .writeInt(0b00000000_00000000_00000000_01100100)
            .writeMedium(0b00000000_00000000_00000000)
            .writeBytes(data, 0, data.readableBytes()));
  }

  @Override
  public Tuple2<ExtensionFrame, ByteBuf> getFrameWithMetadata() {
    ByteBuf metadata = getRandomByteBuf(2);
    ByteBuf data = getRandomByteBuf(2);

    ExtensionFrame frame =
        createExtensionFrame(
            Unpooled.buffer(13)
                .writeShort(0b11111101_00000000)
                .writeInt(0b00000000_00000000_00000000_01100100)
                .writeMedium(0b00000000_00000000_00000010)
                .writeBytes(metadata, 0, metadata.readableBytes())
                .writeBytes(data, 0, data.readableBytes()));

    return Tuples.of(frame, metadata);
  }

  @Override
  public ExtensionFrame getFrameWithoutMetadata() {
    ByteBuf data = getRandomByteBuf(2);

    return createExtensionFrame(
        Unpooled.buffer(11)
            .writeShort(0b11111100_00000000)
            .writeInt(0b00000000_00000000_00000000_01100100)
            .writeMedium(0b00000000_00000000_00000000)
            .writeBytes(data, 0, data.readableBytes()));
  }

  @DisplayName("creates EXT frame with data")
  @Test
  void createExtensionFrameData() {
    ByteBuf data = getRandomByteBuf(2);

    ByteBuf expected =
        Unpooled.buffer(11)
            .writeShort(0b11111100_00000000)
            .writeInt(0b00000000_00000000_00000000_01100100)
            .writeMedium(0b00000000_00000000_00000000)
            .writeBytes(data, 0, data.readableBytes());

    assertThat(createExtensionFrame(DEFAULT, false, 100, null, data).mapFrame(Function.identity()))
        .isEqualTo(expected);
  }

  @DisplayName("creates EXT frame with ignore flag")
  @Test
  void createExtensionFrameIgnore() {
    ByteBuf expected =
        Unpooled.buffer(9)
            .writeShort(0b11111110_00000000)
            .writeInt(0b00000000_00000000_00000000_01100100)
            .writeMedium(0b00000000_00000000_00000000);

    assertThat(
            createExtensionFrame(DEFAULT, true, 100, (ByteBuf) null, null)
                .mapFrame(Function.identity()))
        .isEqualTo(expected);
  }

  @DisplayName("creates EXT frame with metadata")
  @Test
  void createExtensionFrameMetadata() {
    ByteBuf metadata = getRandomByteBuf(2);

    ByteBuf expected =
        Unpooled.buffer(11)
            .writeShort(0b11111101_00000000)
            .writeInt(0b00000000_00000000_00000000_01100100)
            .writeMedium(0b00000000_00000000_00000010)
            .writeBytes(metadata, 0, metadata.readableBytes());

    assertThat(
            createExtensionFrame(DEFAULT, false, 100, metadata, null).mapFrame(Function.identity()))
        .isEqualTo(expected);
  }

  @DisplayName("creates EXT frame with metadata and data")
  @Test
  void createExtensionFrameMetadataAndData() {
    ByteBuf metadata = getRandomByteBuf(2);
    ByteBuf data = getRandomByteBuf(2);

    ByteBuf expected =
        Unpooled.buffer(13)
            .writeShort(0b11111101_00000000)
            .writeInt(0b00000000_00000000_00000000_01100100)
            .writeMedium(0b00000000_00000000_00000010)
            .writeBytes(metadata, 0, metadata.readableBytes())
            .writeBytes(data, 0, data.readableBytes());

    assertThat(
            createExtensionFrame(DEFAULT, false, 100, metadata, data).mapFrame(Function.identity()))
        .isEqualTo(expected);
  }

  @DisplayName("createExtensionFrame throws NullPointerException with null byteBufAllocator")
  @Test
  void createExtensionFrameNullByteBuf() {
    assertThatNullPointerException()
        .isThrownBy(() -> createExtensionFrame(null, true, 100, (ByteBuf) null, null))
        .withMessage("byteBufAllocator must not be null");
  }

  @DisplayName("returns extended type")
  @Test
  void getExtendedType() {
    ExtensionFrame frame =
        createExtensionFrame(
            Unpooled.buffer(9)
                .writeShort(0b11111100_00000000)
                .writeInt(0b00000000_00000000_00000000_01100100)
                .writeMedium(0b00000000_00000000_00000000));

    assertThat(frame.getExtendedType()).isEqualTo(100);
  }

  @DisplayName("tests ignore flag not set")
  @Test
  void isIgnoreFlagSetFalse() {
    ExtensionFrame frame =
        createExtensionFrame(
            Unpooled.buffer(11)
                .writeShort(0b11111100_00000000)
                .writeInt(0b00000000_00000000_00000000_01100100)
                .writeMedium(0b00000000_00000000_00000000));

    assertThat(frame.isIgnoreFlagSet()).isFalse();
  }

  @DisplayName("tests ignore flag set")
  @Test
  void isIgnoreFlagSetTrue() {
    ExtensionFrame frame =
        createExtensionFrame(
            Unpooled.buffer(11)
                .writeShort(0b11111110_00000000)
                .writeInt(0b00000000_00000000_00000000_01100100)
                .writeMedium(0b00000000_00000000_00000000));

    assertThat(frame.isIgnoreFlagSet()).isTrue();
  }
}
