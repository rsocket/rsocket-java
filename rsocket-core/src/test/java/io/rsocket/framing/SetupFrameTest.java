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
import static io.rsocket.framing.SetupFrame.createSetupFrame;
import static io.rsocket.test.util.ByteBufUtils.getRandomByteBuf;
import static io.rsocket.test.util.StringUtils.getRandomString;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.time.Duration;
import java.util.function.Function;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@Disabled
final class SetupFrameTest implements MetadataAndDataFrameTest<SetupFrame> {

  @Override
  public Function<ByteBuf, SetupFrame> getCreateFrameFromByteBuf() {
    return SetupFrame::createSetupFrame;
  }

  @Override
  public Tuple2<SetupFrame, ByteBuf> getFrame() {
    ByteBuf resumeIdentificationToken = getRandomByteBuf(2);
    String metadataMimeType = getRandomString(2);
    ByteBuf metadataMimeTypeBuf = Unpooled.copiedBuffer(metadataMimeType, UTF_8);
    String dataMimeType = getRandomString(3);
    ByteBuf dataMimeTypeBuf = Unpooled.copiedBuffer(dataMimeType, UTF_8);
    ByteBuf metadata = getRandomByteBuf(2);
    ByteBuf data = getRandomByteBuf(2);

    ByteBuf byteBuf =
        Unpooled.buffer(32)
            .writeShort(0b00000101_11000000)
            .writeShort(0b00000000_01100100)
            .writeShort(0b00000000_11001000)
            .writeInt(0b00000000_00000000_0000001_00101100)
            .writeInt(0b00000000_00000000_0000001_10010000)
            .writeShort(0b00000000_00000010)
            .writeBytes(resumeIdentificationToken, 0, resumeIdentificationToken.readableBytes())
            .writeByte(0b00000010)
            .writeBytes(metadataMimeTypeBuf, 0, metadataMimeTypeBuf.readableBytes())
            .writeByte(0b00000011)
            .writeBytes(dataMimeTypeBuf, 0, dataMimeTypeBuf.readableBytes())
            .writeMedium(0b00000000_00000000_00000010)
            .writeBytes(metadata, 0, metadata.readableBytes())
            .writeBytes(data, 0, data.readableBytes());

    SetupFrame frame =
        createSetupFrame(
            DEFAULT,
            true,
            100,
            200,
            Duration.ofMillis(300),
            Duration.ofMillis(400),
            resumeIdentificationToken,
            metadataMimeType,
            dataMimeType,
            metadata,
            data);

    return Tuples.of(frame, byteBuf);
  }

  @Override
  public Tuple2<SetupFrame, ByteBuf> getFrameWithData() {
    ByteBuf resumeIdentificationToken = getRandomByteBuf(2);
    ByteBuf metadataMimeType = getRandomByteBuf(2);
    ByteBuf dataMimeType = getRandomByteBuf(3);
    ByteBuf data = getRandomByteBuf(2);

    SetupFrame frame =
        createSetupFrame(
            Unpooled.buffer(30)
                .writeShort(0b00000101_11000000)
                .writeShort(0b00000000_01100100)
                .writeShort(0b00000000_11001000)
                .writeInt(0b00000000_00000000_0000001_00101100)
                .writeInt(0b00000000_00000000_0000001_10010000)
                .writeShort(0b00000000_00000010)
                .writeBytes(resumeIdentificationToken, 0, resumeIdentificationToken.readableBytes())
                .writeByte(0b00000010)
                .writeBytes(metadataMimeType, 0, metadataMimeType.readableBytes())
                .writeByte(0b00000011)
                .writeBytes(dataMimeType, 0, dataMimeType.readableBytes())
                .writeMedium(0b00000000_00000000_00000000)
                .writeBytes(data, 0, data.readableBytes()));

    return Tuples.of(frame, data);
  }

  @Override
  public SetupFrame getFrameWithEmptyData() {
    ByteBuf resumeIdentificationToken = getRandomByteBuf(2);
    ByteBuf metadataMimeType = getRandomByteBuf(2);
    ByteBuf dataMimeType = getRandomByteBuf(3);

    return createSetupFrame(
        Unpooled.buffer(28)
            .writeShort(0b00000100_11000000)
            .writeShort(0b00000000_01100100)
            .writeShort(0b00000000_11001000)
            .writeInt(0b00000000_00000000_0000001_00101100)
            .writeInt(0b00000000_00000000_0000001_10010000)
            .writeShort(0b00000000_00000010)
            .writeBytes(resumeIdentificationToken, 0, resumeIdentificationToken.readableBytes())
            .writeByte(0b00000010)
            .writeBytes(metadataMimeType, 0, metadataMimeType.readableBytes())
            .writeByte(0b00000011)
            .writeBytes(dataMimeType, 0, dataMimeType.readableBytes())
            .writeMedium(0b00000000_00000000_0000000));
  }

  @Override
  public SetupFrame getFrameWithEmptyMetadata() {
    ByteBuf resumeIdentificationToken = getRandomByteBuf(2);
    ByteBuf metadataMimeType = getRandomByteBuf(2);
    ByteBuf dataMimeType = getRandomByteBuf(3);

    return createSetupFrame(
        Unpooled.buffer(28)
            .writeShort(0b00000101_11000000)
            .writeShort(0b00000000_01100100)
            .writeShort(0b00000000_11001000)
            .writeInt(0b00000000_00000000_0000001_00101100)
            .writeInt(0b00000000_00000000_0000001_10010000)
            .writeShort(0b00000000_00000010)
            .writeBytes(resumeIdentificationToken, 0, resumeIdentificationToken.readableBytes())
            .writeByte(0b00000010)
            .writeBytes(metadataMimeType, 0, metadataMimeType.readableBytes())
            .writeByte(0b00000011)
            .writeBytes(dataMimeType, 0, dataMimeType.readableBytes())
            .writeMedium(0b00000000_00000000_00000000));
  }

  @Override
  public Tuple2<SetupFrame, ByteBuf> getFrameWithMetadata() {
    ByteBuf resumeIdentificationToken = getRandomByteBuf(2);
    ByteBuf metadataMimeType = getRandomByteBuf(2);
    ByteBuf dataMimeType = getRandomByteBuf(3);
    ByteBuf metadata = getRandomByteBuf(2);

    SetupFrame frame =
        createSetupFrame(
            Unpooled.buffer(30)
                .writeShort(0b00000101_11000000)
                .writeShort(0b00000000_01100100)
                .writeShort(0b00000000_11001000)
                .writeInt(0b00000000_00000000_0000001_00101100)
                .writeInt(0b00000000_00000000_0000001_10010000)
                .writeShort(0b00000000_00000010)
                .writeBytes(resumeIdentificationToken, 0, resumeIdentificationToken.readableBytes())
                .writeByte(0b00000010)
                .writeBytes(metadataMimeType, 0, metadataMimeType.readableBytes())
                .writeByte(0b00000011)
                .writeBytes(dataMimeType, 0, dataMimeType.readableBytes())
                .writeMedium(0b00000000_00000000_00000010)
                .writeBytes(metadata, 0, metadata.readableBytes()));

    return Tuples.of(frame, metadata);
  }

  @Override
  public SetupFrame getFrameWithoutMetadata() {
    ByteBuf resumeIdentificationToken = getRandomByteBuf(2);
    ByteBuf metadataMimeType = getRandomByteBuf(2);
    ByteBuf dataMimeType = getRandomByteBuf(3);

    return createSetupFrame(
        Unpooled.buffer(27)
            .writeShort(0b00000100_11000000)
            .writeShort(0b00000000_01100100)
            .writeShort(0b00000000_11001000)
            .writeInt(0b00000000_00000000_0000001_00101100)
            .writeInt(0b00000000_00000000_0000001_10010000)
            .writeShort(0b00000000_00000010)
            .writeBytes(resumeIdentificationToken, 0, resumeIdentificationToken.readableBytes())
            .writeByte(0b00000010)
            .writeBytes(metadataMimeType, 0, metadataMimeType.readableBytes())
            .writeByte(0b00000011)
            .writeBytes(dataMimeType, 0, dataMimeType.readableBytes())
            .writeMedium(0b00000000_00000000_00000000));
  }

  @DisplayName("createSetup throws IllegalArgumentException with invalid keepAliveInterval")
  @Test
  void createSetupFrameInvalidKeepAliveInterval() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                createSetupFrame(
                    DEFAULT,
                    true,
                    Duration.ZERO,
                    Duration.ofMillis(1),
                    null,
                    "",
                    "",
                    (ByteBuf) null,
                    null))
        .withMessage("keepAliveInterval must be a positive duration");
  }

  @DisplayName("createSetup throws IllegalArgumentException with invalid maxLifetime")
  @Test
  void createSetupFrameInvalidMaxLifetime() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                createSetupFrame(
                    DEFAULT,
                    true,
                    Duration.ofMillis(1),
                    Duration.ZERO,
                    null,
                    "",
                    "",
                    (ByteBuf) null,
                    null))
        .withMessage("maxLifetime must be a positive duration");
  }

  @DisplayName("createSetup throws NullPointerException with null byteBufAllocator")
  @Test
  void createSetupFrameNullByteBufAllocator() {
    assertThatNullPointerException()
        .isThrownBy(
            () ->
                createSetupFrame(
                    null,
                    true,
                    Duration.ofMillis(1),
                    Duration.ofMillis(1),
                    null,
                    "",
                    "",
                    (ByteBuf) null,
                    null))
        .withMessage("byteBufAllocator must not be null");
  }

  @DisplayName("createSetup throws NullPointerException with null dataMimeType")
  @Test
  void createSetupFrameNullDataMimeType() {
    assertThatNullPointerException()
        .isThrownBy(
            () ->
                createSetupFrame(
                    DEFAULT,
                    true,
                    Duration.ofMillis(1),
                    Duration.ofMillis(1),
                    null,
                    "",
                    null,
                    (ByteBuf) null,
                    null))
        .withMessage("dataMimeType must not be null");
  }

  @DisplayName("createSetup throws NullPointerException with null keepAliveInterval")
  @Test
  void createSetupFrameNullKeepAliveInterval() {
    assertThatNullPointerException()
        .isThrownBy(
            () ->
                createSetupFrame(
                    DEFAULT, true, null, Duration.ofMillis(1), null, "", "", (ByteBuf) null, null))
        .withMessage("keepAliveInterval must not be null");
  }

  @DisplayName("createSetup throws NullPointerException with null maxLifetime")
  @Test
  void createSetupFrameNullMaxLifetime() {
    assertThatNullPointerException()
        .isThrownBy(
            () ->
                createSetupFrame(
                    DEFAULT, true, Duration.ofMillis(1), null, null, "", "", (ByteBuf) null, null))
        .withMessage("maxLifetime must not be null");
  }

  @DisplayName("createSetup throws NullPointerException with null metadataMimeType")
  @Test
  void createSetupFrameNullMetadataMimeType() {
    assertThatNullPointerException()
        .isThrownBy(
            () ->
                createSetupFrame(
                    DEFAULT,
                    true,
                    Duration.ofMillis(1),
                    Duration.ofMillis(1),
                    null,
                    null,
                    "",
                    (ByteBuf) null,
                    null))
        .withMessage("metadataMimeType must not be null");
  }

  @DisplayName("creates SETUP frame with data")
  @Test
  void createSetupFrameWithData() {
    ByteBuf resumeIdentificationToken = getRandomByteBuf(2);
    String metadataMimeType = getRandomString(2);
    ByteBuf metadataMimeTypeBuf = Unpooled.copiedBuffer(metadataMimeType, UTF_8);
    String dataMimeType = getRandomString(3);
    ByteBuf dataMimeTypeBuf = Unpooled.copiedBuffer(dataMimeType, UTF_8);
    ByteBuf data = getRandomByteBuf(2);

    ByteBuf expected =
        Unpooled.buffer(30)
            .writeShort(0b00000100_11000000)
            .writeShort(0b00000000_01100100)
            .writeShort(0b00000000_11001000)
            .writeInt(0b00000000_00000000_0000001_00101100)
            .writeInt(0b00000000_00000000_0000001_10010000)
            .writeShort(0b00000000_00000010)
            .writeBytes(resumeIdentificationToken, 0, resumeIdentificationToken.readableBytes())
            .writeByte(0b00000010)
            .writeBytes(metadataMimeTypeBuf, 0, metadataMimeTypeBuf.readableBytes())
            .writeByte(0b00000011)
            .writeBytes(dataMimeTypeBuf, 0, dataMimeTypeBuf.readableBytes())
            .writeMedium(0b00000000_00000000_00000000)
            .writeBytes(data, 0, data.readableBytes());

    SetupFrame frame =
        createSetupFrame(
            DEFAULT,
            true,
            100,
            200,
            Duration.ofMillis(300),
            Duration.ofMillis(400),
            resumeIdentificationToken,
            metadataMimeType,
            dataMimeType,
            null,
            data);

    assertThat(frame.mapFrame(Function.identity())).isEqualTo(expected);
  }

  @DisplayName("creates SETUP frame with metadata")
  @Test
  void createSetupFrameWithMetadata() {
    ByteBuf resumeIdentificationToken = getRandomByteBuf(2);
    String metadataMimeType = getRandomString(2);
    ByteBuf metadataMimeTypeBuf = Unpooled.copiedBuffer(metadataMimeType, UTF_8);
    String dataMimeType = getRandomString(3);
    ByteBuf dataMimeTypeBuf = Unpooled.copiedBuffer(dataMimeType, UTF_8);
    ByteBuf metadata = getRandomByteBuf(2);

    ByteBuf expected =
        Unpooled.buffer(30)
            .writeShort(0b00000101_11000000)
            .writeShort(0b00000000_01100100)
            .writeShort(0b00000000_11001000)
            .writeInt(0b00000000_00000000_0000001_00101100)
            .writeInt(0b00000000_00000000_0000001_10010000)
            .writeShort(0b00000000_00000010)
            .writeBytes(resumeIdentificationToken, 0, resumeIdentificationToken.readableBytes())
            .writeByte(0b00000010)
            .writeBytes(metadataMimeTypeBuf, 0, metadataMimeTypeBuf.readableBytes())
            .writeByte(0b00000011)
            .writeBytes(dataMimeTypeBuf, 0, dataMimeTypeBuf.readableBytes())
            .writeMedium(0b00000000_00000000_00000010)
            .writeBytes(metadata, 0, metadata.readableBytes());

    SetupFrame frame =
        createSetupFrame(
            DEFAULT,
            true,
            100,
            200,
            Duration.ofMillis(300),
            Duration.ofMillis(400),
            resumeIdentificationToken,
            metadataMimeType,
            dataMimeType,
            metadata,
            null);

    assertThat(frame.mapFrame(Function.identity())).isEqualTo(expected);
  }

  @DisplayName("creates SETUP frame with resume identification token")
  @Test
  void createSetupFrameWithResumeIdentificationToken() {
    ByteBuf resumeIdentificationToken = getRandomByteBuf(2);
    String metadataMimeType = getRandomString(2);
    ByteBuf metadataMimeTypeBuf = Unpooled.copiedBuffer(metadataMimeType, UTF_8);
    String dataMimeType = getRandomString(3);
    ByteBuf dataMimeTypeBuf = Unpooled.copiedBuffer(dataMimeType, UTF_8);

    ByteBuf expected =
        Unpooled.buffer(32)
            .writeShort(0b00000100_11000000)
            .writeShort(0b00000000_01100100)
            .writeShort(0b00000000_11001000)
            .writeInt(0b00000000_00000000_0000001_00101100)
            .writeInt(0b00000000_00000000_0000001_10010000)
            .writeShort(0b00000000_00000010)
            .writeBytes(resumeIdentificationToken, 0, resumeIdentificationToken.readableBytes())
            .writeByte(0b00000010)
            .writeBytes(metadataMimeTypeBuf, 0, metadataMimeTypeBuf.readableBytes())
            .writeByte(0b00000011)
            .writeBytes(dataMimeTypeBuf, 0, dataMimeTypeBuf.readableBytes())
            .writeMedium(0b00000000_00000000_00000000);

    SetupFrame frame =
        createSetupFrame(
            DEFAULT,
            true,
            100,
            200,
            Duration.ofMillis(300),
            Duration.ofMillis(400),
            resumeIdentificationToken,
            metadataMimeType,
            dataMimeType,
            null,
            null);

    assertThat(frame.mapFrame(Function.identity())).isEqualTo(expected);
  }

  @DisplayName("creates SETUP frame without data")
  @Test
  void createSetupFrameWithoutData() {
    ByteBuf resumeIdentificationToken = getRandomByteBuf(2);
    String metadataMimeType = getRandomString(2);
    ByteBuf metadataMimeTypeBuf = Unpooled.copiedBuffer(metadataMimeType, UTF_8);
    String dataMimeType = getRandomString(3);
    ByteBuf dataMimeTypeBuf = Unpooled.copiedBuffer(dataMimeType, UTF_8);

    ByteBuf expected =
        Unpooled.buffer(30)
            .writeShort(0b00000100_11000000)
            .writeShort(0b00000000_01100100)
            .writeShort(0b00000000_11001000)
            .writeInt(0b00000000_00000000_0000001_00101100)
            .writeInt(0b00000000_00000000_0000001_10010000)
            .writeShort(0b00000000_00000010)
            .writeBytes(resumeIdentificationToken, 0, resumeIdentificationToken.readableBytes())
            .writeByte(0b00000010)
            .writeBytes(metadataMimeTypeBuf, 0, metadataMimeTypeBuf.readableBytes())
            .writeByte(0b00000011)
            .writeBytes(dataMimeTypeBuf, 0, dataMimeTypeBuf.readableBytes())
            .writeMedium(0b00000000_00000000_00000000);

    SetupFrame frame =
        createSetupFrame(
            DEFAULT,
            true,
            100,
            200,
            Duration.ofMillis(300),
            Duration.ofMillis(400),
            resumeIdentificationToken,
            metadataMimeType,
            dataMimeType,
            null,
            null);

    assertThat(frame.mapFrame(Function.identity())).isEqualTo(expected);
  }

  @DisplayName("creates SETUP frame without metadata")
  @Test
  void createSetupFrameWithoutMetadata() {
    ByteBuf resumeIdentificationToken = getRandomByteBuf(2);
    String metadataMimeType = getRandomString(2);
    ByteBuf metadataMimeTypeBuf = Unpooled.copiedBuffer(metadataMimeType, UTF_8);
    String dataMimeType = getRandomString(3);
    ByteBuf dataMimeTypeBuf = Unpooled.copiedBuffer(dataMimeType, UTF_8);

    ByteBuf expected =
        Unpooled.buffer(30)
            .writeShort(0b00000100_11000000)
            .writeShort(0b00000000_01100100)
            .writeShort(0b00000000_11001000)
            .writeInt(0b00000000_00000000_0000001_00101100)
            .writeInt(0b00000000_00000000_0000001_10010000)
            .writeShort(0b00000000_00000010)
            .writeBytes(resumeIdentificationToken, 0, resumeIdentificationToken.readableBytes())
            .writeByte(0b00000010)
            .writeBytes(metadataMimeTypeBuf, 0, metadataMimeTypeBuf.readableBytes())
            .writeByte(0b00000011)
            .writeBytes(dataMimeTypeBuf, 0, dataMimeTypeBuf.readableBytes())
            .writeMedium(0b00000000_00000000_00000000);

    SetupFrame frame =
        createSetupFrame(
            DEFAULT,
            true,
            100,
            200,
            Duration.ofMillis(300),
            Duration.ofMillis(400),
            resumeIdentificationToken,
            metadataMimeType,
            dataMimeType,
            null,
            null);

    assertThat(frame.mapFrame(Function.identity())).isEqualTo(expected);
  }

  @DisplayName("creates SETUP frame without resume identification token")
  @Test
  void createSetupFrameWithoutResumeIdentificationToken() {
    String metadataMimeType = getRandomString(2);
    ByteBuf metadataMimeTypeBuf = Unpooled.copiedBuffer(metadataMimeType, UTF_8);
    String dataMimeType = getRandomString(3);
    ByteBuf dataMimeTypeBuf = Unpooled.copiedBuffer(dataMimeType, UTF_8);

    ByteBuf expected =
        Unpooled.buffer(32)
            .writeShort(0b00000100_01000000)
            .writeShort(0b00000000_01100100)
            .writeShort(0b00000000_11001000)
            .writeInt(0b00000000_00000000_0000001_00101100)
            .writeInt(0b00000000_00000000_0000001_10010000)
            .writeByte(0b00000010)
            .writeBytes(metadataMimeTypeBuf, 0, metadataMimeTypeBuf.readableBytes())
            .writeByte(0b00000011)
            .writeBytes(dataMimeTypeBuf, 0, dataMimeTypeBuf.readableBytes())
            .writeMedium(0b00000000_00000000_00000000);

    SetupFrame frame =
        createSetupFrame(
            DEFAULT,
            true,
            100,
            200,
            Duration.ofMillis(300),
            Duration.ofMillis(400),
            null,
            metadataMimeType,
            dataMimeType,
            null,
            null);

    assertThat(frame.mapFrame(Function.identity())).isEqualTo(expected);
  }

  @DisplayName("returns data mime type")
  @Test
  void getDataMimeType() {
    String metadataMimeType = getRandomString(2);
    ByteBuf metadataMimeTypeBuf = Unpooled.copiedBuffer(metadataMimeType, UTF_8);
    String dataMimeType = getRandomString(3);
    ByteBuf dataMimeTypeBuf = Unpooled.copiedBuffer(dataMimeType, UTF_8);

    SetupFrame frame =
        createSetupFrame(
            Unpooled.buffer(23)
                .writeShort(0b00000100_01000000)
                .writeShort(0b00000000_01100100)
                .writeShort(0b00000000_11001000)
                .writeInt(0b00000000_00000000_0000001_00101100)
                .writeInt(0b00000000_00000000_0000001_10010000)
                .writeByte(0b00000010)
                .writeBytes(metadataMimeTypeBuf, 0, metadataMimeTypeBuf.readableBytes())
                .writeByte(0b00000011)
                .writeBytes(dataMimeTypeBuf, 0, dataMimeTypeBuf.readableBytes())
                .writeMedium(0b00000000_00000000_00000000));

    assertThat(frame.getDataMimeType()).isEqualTo(dataMimeType);
  }

  @DisplayName("returns the keepalive interval")
  @Test
  void getKeepaliveInterval() {
    ByteBuf metadataMimeType = getRandomByteBuf(2);
    ByteBuf dataMimeType = getRandomByteBuf(3);

    SetupFrame frame =
        createSetupFrame(
            Unpooled.buffer(23)
                .writeShort(0b00000100_01000000)
                .writeShort(0b00000000_01100100)
                .writeShort(0b00000000_11001000)
                .writeInt(0b00000000_00000000_0000001_00101100)
                .writeInt(0b00000000_00000000_0000001_10010000)
                .writeByte(0b00000010)
                .writeBytes(metadataMimeType, 0, metadataMimeType.readableBytes())
                .writeByte(0b00000011)
                .writeBytes(dataMimeType, 0, dataMimeType.readableBytes())
                .writeMedium(0b00000000_00000000_00000000));

    assertThat(frame.getKeepAliveInterval()).isEqualTo(Duration.ofMillis(300));
  }

  @DisplayName("returns major version")
  @Test
  void getMajorVersion() {
    ByteBuf metadataMimeType = getRandomByteBuf(2);
    ByteBuf dataMimeType = getRandomByteBuf(3);

    SetupFrame frame =
        createSetupFrame(
            Unpooled.buffer(23)
                .writeShort(0b00000100_01000000)
                .writeShort(0b00000000_01100100)
                .writeShort(0b00000000_11001000)
                .writeInt(0b00000000_00000000_0000001_00101100)
                .writeInt(0b00000000_00000000_0000001_10010000)
                .writeByte(0b00000010)
                .writeBytes(metadataMimeType, 0, metadataMimeType.readableBytes())
                .writeByte(0b00000011)
                .writeBytes(dataMimeType, 0, dataMimeType.readableBytes())
                .writeMedium(0b00000000_00000000_00000000));

    assertThat(frame.getMajorVersion()).isEqualTo(100);
  }

  @DisplayName("returns the max lifetime")
  @Test
  void getMaxLifetime() {
    ByteBuf metadataMimeType = getRandomByteBuf(2);
    ByteBuf dataMimeType = getRandomByteBuf(3);

    SetupFrame frame =
        createSetupFrame(
            Unpooled.buffer(23)
                .writeShort(0b00000100_01000000)
                .writeShort(0b00000000_01100100)
                .writeShort(0b00000000_11001000)
                .writeInt(0b00000000_00000000_0000001_00101100)
                .writeInt(0b00000000_00000000_0000001_10010000)
                .writeByte(0b00000010)
                .writeBytes(metadataMimeType, 0, metadataMimeType.readableBytes())
                .writeByte(0b00000011)
                .writeBytes(dataMimeType, 0, dataMimeType.readableBytes())
                .writeMedium(0b00000000_00000000_00000000));

    assertThat(frame.getMaxLifetime()).isEqualTo(Duration.ofMillis(400));
  }

  @DisplayName("returns metadata mime type")
  @Test
  void getMetadataMimeType() {
    String metadataMimeType = getRandomString(2);
    ByteBuf metadataMimeTypeBuf = Unpooled.copiedBuffer(metadataMimeType, UTF_8);
    String dataMimeType = getRandomString(3);
    ByteBuf dataMimeTypeBuf = Unpooled.copiedBuffer(dataMimeType, UTF_8);

    SetupFrame frame =
        createSetupFrame(
            Unpooled.buffer(23)
                .writeShort(0b00000100_01000000)
                .writeShort(0b00000000_01100100)
                .writeShort(0b00000000_11001000)
                .writeInt(0b00000000_00000000_0000001_00101100)
                .writeInt(0b00000000_00000000_0000001_10010000)
                .writeByte(0b00000010)
                .writeBytes(metadataMimeTypeBuf, 0, metadataMimeTypeBuf.readableBytes())
                .writeByte(0b00000011)
                .writeBytes(dataMimeTypeBuf, 0, dataMimeTypeBuf.readableBytes())
                .writeMedium(0b00000000_00000000_00000000));

    assertThat(frame.getMetadataMimeType()).isEqualTo(metadataMimeType);
  }

  @DisplayName("returns minor version")
  @Test
  void getMinorVersion() {
    ByteBuf metadataMimeType = getRandomByteBuf(2);
    ByteBuf dataMimeType = getRandomByteBuf(3);

    SetupFrame frame =
        createSetupFrame(
            Unpooled.buffer(23)
                .writeShort(0b00000100_01000000)
                .writeShort(0b00000000_01100100)
                .writeShort(0b00000000_11001000)
                .writeInt(0b00000000_00000000_0000001_00101100)
                .writeInt(0b00000000_00000000_0000001_10010000)
                .writeByte(0b00000010)
                .writeBytes(metadataMimeType, 0, metadataMimeType.readableBytes())
                .writeByte(0b00000011)
                .writeBytes(dataMimeType, 0, dataMimeType.readableBytes())
                .writeMedium(0b00000000_00000000_00000000));

    assertThat(frame.getMinorVersion()).isEqualTo(200);
  }

  @DisplayName("tests lease flag not set")
  @Test
  void isLeaseFlagSetFalse() {
    ByteBuf metadataMimeType = getRandomByteBuf(2);
    ByteBuf dataMimeType = getRandomByteBuf(3);

    SetupFrame frame =
        createSetupFrame(
            Unpooled.buffer(23)
                .writeShort(0b00000100_00000000)
                .writeShort(0b00000000_01100100)
                .writeShort(0b00000000_11001000)
                .writeInt(0b00000000_00000000_0000001_00101100)
                .writeInt(0b00000000_00000000_0000001_10010000)
                .writeByte(0b00000010)
                .writeBytes(metadataMimeType, 0, metadataMimeType.readableBytes())
                .writeByte(0b00000011)
                .writeBytes(dataMimeType, 0, dataMimeType.readableBytes())
                .writeMedium(0b00000000_00000000_00000000));

    assertThat(frame.isLeaseFlagSet()).isFalse();
  }

  @DisplayName("test lease flag set")
  @Test
  void isLeaseFlagSetTrue() {
    ByteBuf metadataMimeType = getRandomByteBuf(2);
    ByteBuf dataMimeType = getRandomByteBuf(3);

    SetupFrame frame =
        createSetupFrame(
            Unpooled.buffer(23)
                .writeShort(0b00000100_01000000)
                .writeShort(0b00000000_01100100)
                .writeShort(0b00000000_11001000)
                .writeInt(0b00000000_00000000_0000001_00101100)
                .writeInt(0b00000000_00000000_0000001_10010000)
                .writeByte(0b00000010)
                .writeBytes(metadataMimeType, 0, metadataMimeType.readableBytes())
                .writeByte(0b00000011)
                .writeBytes(dataMimeType, 0, dataMimeType.readableBytes())
                .writeMedium(0b00000000_00000000_00000000));

    assertThat(frame.isLeaseFlagSet()).isTrue();
  }

  @DisplayName("maps empty optional for resume identification token")
  @Test
  void mapResumeIdentificationNoFlag() {
    ByteBuf metadataMimeType = getRandomByteBuf(2);
    ByteBuf dataMimeType = getRandomByteBuf(3);

    SetupFrame frame =
        createSetupFrame(
            Unpooled.buffer(28)
                .writeShort(0b00000101_01000000)
                .writeShort(0b00000000_01100100)
                .writeShort(0b00000000_11001000)
                .writeInt(0b00000000_00000000_0000001_00101100)
                .writeInt(0b00000000_00000000_0000001_10010000)
                .writeByte(0b00000010)
                .writeBytes(metadataMimeType, 0, metadataMimeType.readableBytes())
                .writeByte(0b00000011)
                .writeBytes(dataMimeType, 0, dataMimeType.readableBytes())
                .writeMedium(0b00000000_00000000_00000000));

    assertThat(frame.mapResumeIdentificationToken(Function.identity())).isEmpty();
  }

  @DisplayName("maps resume identification token")
  @Test
  void mapResumeIdentificationToken() {
    ByteBuf resumeIdentificationToken = getRandomByteBuf(2);
    ByteBuf metadataMimeType = getRandomByteBuf(2);
    ByteBuf dataMimeType = getRandomByteBuf(3);

    SetupFrame frame =
        createSetupFrame(
            Unpooled.buffer(28)
                .writeShort(0b00000101_11000000)
                .writeShort(0b00000000_01100100)
                .writeShort(0b00000000_11001000)
                .writeInt(0b00000000_00000000_0000001_00101100)
                .writeInt(0b00000000_00000000_0000001_10010000)
                .writeShort(0b00000000_00000010)
                .writeBytes(resumeIdentificationToken, 0, resumeIdentificationToken.readableBytes())
                .writeByte(0b00000010)
                .writeBytes(metadataMimeType, 0, metadataMimeType.readableBytes())
                .writeByte(0b00000011)
                .writeBytes(dataMimeType, 0, dataMimeType.readableBytes())
                .writeMedium(0b00000000_00000000_00000000));

    assertThat(frame.mapResumeIdentificationToken(Function.identity()))
        .hasValue(resumeIdentificationToken);
  }

  @DisplayName("maps empty resume identification token")
  @Test
  void mapResumeIdentificationTokenEmpty() {
    ByteBuf metadataMimeType = getRandomByteBuf(2);
    ByteBuf dataMimeType = getRandomByteBuf(3);

    SetupFrame frame =
        createSetupFrame(
            Unpooled.buffer(28)
                .writeShort(0b00000101_11000000)
                .writeShort(0b00000000_01100100)
                .writeShort(0b00000000_11001000)
                .writeInt(0b00000000_00000000_0000001_00101100)
                .writeInt(0b00000000_00000000_0000001_10010000)
                .writeShort(0b00000000_00000000)
                .writeByte(0b00000010)
                .writeBytes(metadataMimeType, 0, metadataMimeType.readableBytes())
                .writeByte(0b00000011)
                .writeBytes(dataMimeType, 0, dataMimeType.readableBytes())
                .writeMedium(0b00000000_00000000_00000000));

    assertThat(frame.mapResumeIdentificationToken(Function.identity())).hasValue(EMPTY_BUFFER);
  }

  @DisplayName("mapResumeIdentificationToken throws NullPointerException with null function")
  @Test
  void mapResumeIdentificationTokenNullFunction() {
    ByteBuf metadataMimeType = getRandomByteBuf(2);
    ByteBuf dataMimeType = getRandomByteBuf(3);

    SetupFrame frame =
        createSetupFrame(
            Unpooled.buffer(24)
                .writeShort(0b00000101_01000000)
                .writeShort(0b00000000_01100100)
                .writeShort(0b00000000_11001000)
                .writeInt(0b00000000_00000000_0000001_00101100)
                .writeInt(0b00000000_00000000_0000001_10010000)
                .writeByte(0b00000010)
                .writeBytes(metadataMimeType, 0, metadataMimeType.readableBytes())
                .writeByte(0b00000011)
                .writeBytes(dataMimeType, 0, dataMimeType.readableBytes())
                .writeMedium(0b00000000_00000000_00000000));

    assertThatNullPointerException()
        .isThrownBy(() -> frame.mapResumeIdentificationToken(null))
        .withMessage("function must not be null");
  }
}
