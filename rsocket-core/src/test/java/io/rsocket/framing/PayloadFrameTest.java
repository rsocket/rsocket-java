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
import static io.netty.buffer.Unpooled.buffer;
import static io.netty.buffer.UnpooledByteBufAllocator.DEFAULT;
import static io.rsocket.framing.PayloadFrame.createPayloadFrame;
import static io.rsocket.test.util.ByteBufUtils.getRandomByteBuf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.netty.buffer.ByteBuf;
import java.util.function.Function;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@Disabled
final class PayloadFrameTest implements FragmentableFrameTest<PayloadFrame> {

  @Override
  public Function<ByteBuf, PayloadFrame> getCreateFrameFromByteBuf() {
    return PayloadFrame::createPayloadFrame;
  }

  @Override
  public Tuple2<PayloadFrame, ByteBuf> getFrame() {
    ByteBuf byteBuf =
        buffer(9)
            .writeShort(0b00101001_00000000)
            .writeMedium(0b00000000_00000000_00000010)
            .writeBytes(getRandomByteBuf(2))
            .writeBytes(getRandomByteBuf(2));

    PayloadFrame frame = createPayloadFrame(byteBuf);

    return Tuples.of(frame, byteBuf);
  }

  @Override
  public Tuple2<PayloadFrame, ByteBuf> getFrameWithData() {
    ByteBuf data = getRandomByteBuf(2);

    PayloadFrame frame =
        createPayloadFrame(
            buffer(7)
                .writeShort(0b00101000_00000000)
                .writeMedium(0b00000000_00000000_00000000)
                .writeBytes(data, 0, data.readableBytes()));

    return Tuples.of(frame, data);
  }

  @Override
  public PayloadFrame getFrameWithEmptyData() {
    return createPayloadFrame(
        buffer(5).writeShort(0b00101000_00000000).writeMedium(0b00000000_00000000_00000000));
  }

  @Override
  public PayloadFrame getFrameWithEmptyMetadata() {
    return createPayloadFrame(
        buffer(5).writeShort(0b00101001_00000000).writeMedium(0b00000000_00000000_00000000));
  }

  @Override
  public PayloadFrame getFrameWithFollowsFlagSet() {
    return createPayloadFrame(
        buffer(5).writeShort(0b00101000_10000000).writeMedium(0b00000000_00000000_00000000));
  }

  @Override
  public Tuple2<PayloadFrame, ByteBuf> getFrameWithMetadata() {
    ByteBuf metadata = getRandomByteBuf(2);

    PayloadFrame frame =
        createPayloadFrame(
            buffer(7)
                .writeShort(0b00101001_00000000)
                .writeMedium(0b00000000_00000000_00000010)
                .writeBytes(metadata, 0, metadata.readableBytes()));

    return Tuples.of(frame, metadata);
  }

  @Override
  public PayloadFrame getFrameWithoutFollowsFlagSet() {
    return createPayloadFrame(
        buffer(5).writeShort(0b00101000_01000000).writeMedium(0b00000000_00000000_00000000));
  }

  @Override
  public PayloadFrame getFrameWithoutMetadata() {
    return createPayloadFrame(
        buffer(5).writeShort(0b00101000_10000000).writeMedium(0b00000000_00000000_00000000));
  }

  @DisplayName("createPayloadFrame throws IllegalArgumentException without complete flag or data")
  @Test
  void createPayloadFrameNonCompleteNullData() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> createPayloadFrame(DEFAULT, false, false, (ByteBuf) null, null))
        .withMessage("Payload frame must either be complete, have data, or both");
  }

  @DisplayName("createPayloadFrame throws NullPointerException with null byteBufAllocator")
  @Test
  void createPayloadFrameNullByteBufAllocator() {
    assertThatNullPointerException()
        .isThrownBy(() -> createPayloadFrame(null, false, false, null, EMPTY_BUFFER))
        .withMessage("byteBufAllocator must not be null");
  }

  @DisplayName("creates PAYLOAD frame with Complete flag")
  @Test
  void createPayloadFrameWithComplete() {
    ByteBuf expected =
        buffer(5).writeShort(0b00101000_01000000).writeMedium(0b00000000_00000000_00000000);

    PayloadFrame frame = createPayloadFrame(DEFAULT, false, true, (ByteBuf) null, null);

    assertThat(frame.mapFrame(Function.identity())).isEqualTo(expected);
  }

  @DisplayName("creates PAYLOAD frame with data")
  @Test
  void createPayloadFrameWithData() {
    ByteBuf data = getRandomByteBuf(2);

    ByteBuf expected =
        buffer(9)
            .writeShort(0b00101000_00100000)
            .writeMedium(0b00000000_00000000_00000000)
            .writeBytes(data, 0, data.readableBytes());

    PayloadFrame frame = createPayloadFrame(DEFAULT, false, false, null, data);

    assertThat(frame.mapFrame(Function.identity())).isEqualTo(expected);
  }

  @DisplayName("creates PAYLOAD frame with Follows flag")
  @Test
  void createPayloadFrameWithFollows() {
    ByteBuf expected =
        buffer(7).writeShort(0b00101000_10100000).writeMedium(0b00000000_00000000_00000000);

    PayloadFrame frame = createPayloadFrame(DEFAULT, true, false, null, EMPTY_BUFFER);

    assertThat(frame.mapFrame(Function.identity())).isEqualTo(expected);
  }

  @DisplayName("creates PAYLOAD frame with metadata")
  @Test
  void createPayloadFrameWithMetadata() {
    ByteBuf metadata = getRandomByteBuf(2);

    ByteBuf expected =
        buffer(9)
            .writeShort(0b00101001_00100000)
            .writeMedium(0b00000000_00000000_00000010)
            .writeBytes(metadata, 0, metadata.readableBytes());

    PayloadFrame frame = createPayloadFrame(DEFAULT, false, false, metadata, EMPTY_BUFFER);

    assertThat(frame.mapFrame(Function.identity())).isEqualTo(expected);
  }

  @DisplayName("creates PAYLOAD frame with metadata and data")
  @Test
  void createPayloadFrameWithMetadataAnData() {
    ByteBuf metadata = getRandomByteBuf(2);
    ByteBuf data = getRandomByteBuf(2);

    ByteBuf expected =
        buffer(11)
            .writeShort(0b00101001_00100000)
            .writeMedium(0b00000000_00000000_00000010)
            .writeBytes(metadata, 0, metadata.readableBytes())
            .writeBytes(data, 0, data.readableBytes());

    PayloadFrame frame = createPayloadFrame(DEFAULT, false, false, metadata, data);

    assertThat(frame.mapFrame(Function.identity())).isEqualTo(expected);
  }

  @DisplayName("tests complete flag not set")
  @Test
  void isCompleteFlagSetFalse() {
    PayloadFrame frame =
        createPayloadFrame(
            buffer(5).writeShort(0b00101000_00000000).writeMedium(0b00000000_00000000_00000000));

    assertThat(frame.isCompleteFlagSet()).isFalse();
  }

  @DisplayName("tests complete flag set")
  @Test
  void isCompleteFlagSetTrue() {
    PayloadFrame frame =
        createPayloadFrame(
            buffer(5).writeShort(0b00101000_01000000).writeMedium(0b00000000_00000000_00000000));

    assertThat(frame.isCompleteFlagSet()).isTrue();
  }

  @DisplayName("tests next flag set")
  @Test
  void isNextFlagNotSet() {
    PayloadFrame frame =
        createPayloadFrame(
            buffer(5).writeShort(0b00101000_00000000).writeMedium(0b00000000_00000000_00000000));

    assertThat(frame.isNextFlagSet()).isFalse();
  }

  @DisplayName("tests next flag set")
  @Test
  void isNextFlagSetTrue() {
    PayloadFrame frame =
        createPayloadFrame(
            buffer(5).writeShort(0b00101000_00100000).writeMedium(0b00000000_00000000_00000000));

    assertThat(frame.isNextFlagSet()).isTrue();
  }
}
