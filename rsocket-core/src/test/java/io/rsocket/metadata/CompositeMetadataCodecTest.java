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

package io.rsocket.metadata;

import static io.rsocket.metadata.CompositeMetadataCodec.decodeMimeAndContentBuffersSlices;
import static io.rsocket.metadata.CompositeMetadataCodec.decodeMimeIdFromMimeBuffer;
import static io.rsocket.metadata.CompositeMetadataCodec.decodeMimeTypeFromMimeBuffer;
import static org.assertj.core.api.Assertions.*;

import io.netty.buffer.*;
import io.netty.util.CharsetUtil;
import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
import io.rsocket.test.util.ByteBufUtils;
import io.rsocket.util.NumberUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class CompositeMetadataCodecTest {

  final LeaksTrackingByteBufAllocator testAllocator =
      LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);

  @AfterEach
  void tearDownAndCheckForLeaks() {
    testAllocator.assertHasNoLeaks();
  }

  static String byteToBitsString(byte b) {
    return String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0');
  }

  static String toHeaderBits(ByteBuf encoded) {
    encoded.markReaderIndex();
    byte headerByte = encoded.readByte();
    String byteAsString = byteToBitsString(headerByte);
    encoded.resetReaderIndex();
    return byteAsString;
  }
  // ====

  @Test
  void customMimeHeaderLatin1_encodingFails() {
    String mimeNotAscii = "mime/typé";

    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> CompositeMetadataCodec.encodeMetadataHeader(testAllocator, mimeNotAscii, 0))
        .withMessage("custom mime type must be US_ASCII characters only");
  }

  @Test
  void customMimeHeaderLength0_encodingFails() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CompositeMetadataCodec.encodeMetadataHeader(testAllocator, "", 0))
        .withMessage(
            "custom mime type must have a strictly positive length that fits on 7 unsigned bits, ie 1-128");
  }

  @Test
  void customMimeHeaderLength127() {
    StringBuilder builder = new StringBuilder(127);
    for (int i = 0; i < 127; i++) {
      builder.append('a');
    }
    String mimeString = builder.toString();
    ByteBuf encoded = CompositeMetadataCodec.encodeMetadataHeader(testAllocator, mimeString, 0);

    // remember actual length = encoded length + 1
    assertThat(toHeaderBits(encoded)).startsWith("0").isEqualTo("01111110");

    final ByteBuf[] byteBufs = decodeMimeAndContentBuffersSlices(encoded, 0, false);
    assertThat(byteBufs).hasSize(2).doesNotContainNull();

    ByteBuf header = byteBufs[0];
    ByteBuf content = byteBufs[1];
    header.markReaderIndex();

    assertThat(header.readableBytes()).as("metadata header size").isGreaterThan(1);

    assertThat((int) header.readByte())
        .as("mime length")
        .isEqualTo(127 - 1); // encoded as actual length - 1

    assertThat(header.readCharSequence(127, CharsetUtil.US_ASCII))
        .as("mime string")
        .hasToString(mimeString);

    header.resetReaderIndex();
    assertThat(CompositeMetadataCodec.decodeMimeTypeFromMimeBuffer(header))
        .as("decoded mime string")
        .hasToString(mimeString);

    assertThat(content.readableBytes()).as("no metadata content").isZero();
    encoded.release();
  }

  @Test
  void customMimeHeaderLength128() {
    StringBuilder builder = new StringBuilder(128);
    for (int i = 0; i < 128; i++) {
      builder.append('a');
    }
    String mimeString = builder.toString();
    ByteBuf encoded = CompositeMetadataCodec.encodeMetadataHeader(testAllocator, mimeString, 0);

    // remember actual length = encoded length + 1
    assertThat(toHeaderBits(encoded)).startsWith("0").isEqualTo("01111111");

    final ByteBuf[] byteBufs = decodeMimeAndContentBuffersSlices(encoded, 0, false);
    assertThat(byteBufs).hasSize(2).doesNotContainNull();

    ByteBuf header = byteBufs[0];
    ByteBuf content = byteBufs[1];
    header.markReaderIndex();

    assertThat(header.readableBytes()).as("metadata header size").isGreaterThan(1);

    assertThat((int) header.readByte())
        .as("mime length")
        .isEqualTo(128 - 1); // encoded as actual length - 1

    assertThat(header.readCharSequence(128, CharsetUtil.US_ASCII))
        .as("mime string")
        .hasToString(mimeString);

    header.resetReaderIndex();
    assertThat(CompositeMetadataCodec.decodeMimeTypeFromMimeBuffer(header))
        .as("decoded mime string")
        .hasToString(mimeString);

    assertThat(content.readableBytes()).as("no metadata content").isZero();
    encoded.release();
  }

  @Test
  void customMimeHeaderLength129_encodingFails() {
    StringBuilder builder = new StringBuilder(129);
    for (int i = 0; i < 129; i++) {
      builder.append('a');
    }

    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> CompositeMetadataCodec.encodeMetadataHeader(testAllocator, builder.toString(), 0))
        .withMessage(
            "custom mime type must have a strictly positive length that fits on 7 unsigned bits, ie 1-128");
  }

  @Test
  void customMimeHeaderLengthOne() {
    String mimeString = "w";
    ByteBuf encoded = CompositeMetadataCodec.encodeMetadataHeader(testAllocator, mimeString, 0);

    // remember actual length = encoded length + 1
    assertThat(toHeaderBits(encoded)).startsWith("0").isEqualTo("00000000");

    final ByteBuf[] byteBufs = decodeMimeAndContentBuffersSlices(encoded, 0, false);
    assertThat(byteBufs).hasSize(2).doesNotContainNull();

    ByteBuf header = byteBufs[0];
    ByteBuf content = byteBufs[1];
    header.markReaderIndex();

    assertThat(header.readableBytes()).as("metadata header size").isGreaterThan(1);

    assertThat((int) header.readByte()).as("mime length").isZero(); // encoded as actual length - 1

    assertThat(header.readCharSequence(1, CharsetUtil.US_ASCII))
        .as("mime string")
        .hasToString(mimeString);

    header.resetReaderIndex();
    assertThat(CompositeMetadataCodec.decodeMimeTypeFromMimeBuffer(header))
        .as("decoded mime string")
        .hasToString(mimeString);

    assertThat(content.readableBytes()).as("no metadata content").isZero();
    encoded.release();
  }

  @Test
  void customMimeHeaderLengthTwo() {
    String mimeString = "ww";
    ByteBuf encoded = CompositeMetadataCodec.encodeMetadataHeader(testAllocator, mimeString, 0);

    // remember actual length = encoded length + 1
    assertThat(toHeaderBits(encoded)).startsWith("0").isEqualTo("00000001");

    final ByteBuf[] byteBufs = decodeMimeAndContentBuffersSlices(encoded, 0, false);
    assertThat(byteBufs).hasSize(2).doesNotContainNull();

    ByteBuf header = byteBufs[0];
    ByteBuf content = byteBufs[1];
    header.markReaderIndex();

    assertThat(header.readableBytes()).as("metadata header size").isGreaterThan(1);

    assertThat((int) header.readByte())
        .as("mime length")
        .isEqualTo(2 - 1); // encoded as actual length - 1

    assertThat(header.readCharSequence(2, CharsetUtil.US_ASCII))
        .as("mime string")
        .hasToString(mimeString);

    header.resetReaderIndex();
    assertThat(CompositeMetadataCodec.decodeMimeTypeFromMimeBuffer(header))
        .as("decoded mime string")
        .hasToString(mimeString);

    assertThat(content.readableBytes()).as("no metadata content").isZero();
    encoded.release();
  }

  @Test
  void customMimeHeaderUtf8_encodingFails() {
    String mimeNotAscii =
        "mime/tyࠒe"; // this is the SAMARITAN LETTER QUF u+0812 represented on 3 bytes
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> CompositeMetadataCodec.encodeMetadataHeader(testAllocator, mimeNotAscii, 0))
        .withMessage("custom mime type must be US_ASCII characters only");
  }

  @Test
  void decodeEntryAtEndOfBuffer() {
    ByteBuf fakeEntry = Unpooled.buffer();

    assertThatIllegalArgumentException()
        .isThrownBy(() -> decodeMimeAndContentBuffersSlices(fakeEntry, 0, false));
  }

  @Test
  void decodeEntryHasNoContentLength() {
    ByteBuf fakeEntry = Unpooled.buffer();
    fakeEntry.writeByte(0);
    fakeEntry.writeCharSequence("w", CharsetUtil.US_ASCII);

    assertThatIllegalStateException()
        .isThrownBy(() -> decodeMimeAndContentBuffersSlices(fakeEntry, 0, false));
  }

  @Test
  void decodeEntryTooShortForContentLength() {
    ByteBuf fakeEntry = Unpooled.buffer();
    fakeEntry.writeByte(1);
    fakeEntry.writeCharSequence("w", CharsetUtil.US_ASCII);
    NumberUtils.encodeUnsignedMedium(fakeEntry, 456);
    fakeEntry.writeChar('w');

    assertThatIllegalStateException()
        .isThrownBy(() -> decodeMimeAndContentBuffersSlices(fakeEntry, 0, false));
  }

  @Test
  void decodeEntryTooShortForMimeLength() {
    ByteBuf fakeEntry = Unpooled.buffer();
    fakeEntry.writeByte(120);

    assertThatIllegalStateException()
        .isThrownBy(() -> decodeMimeAndContentBuffersSlices(fakeEntry, 0, false));
  }

  @Test
  void decodeIdMinusTwoWhenMoreThanOneByte() {
    ByteBuf fakeIdBuffer = Unpooled.buffer(2);
    fakeIdBuffer.writeInt(200);

    assertThat(decodeMimeIdFromMimeBuffer(fakeIdBuffer))
        .isEqualTo((WellKnownMimeType.UNPARSEABLE_MIME_TYPE.getIdentifier()));
  }

  @Test
  void decodeIdMinusTwoWhenZeroByte() {
    ByteBuf fakeIdBuffer = Unpooled.buffer(0);

    assertThat(decodeMimeIdFromMimeBuffer(fakeIdBuffer))
        .isEqualTo((WellKnownMimeType.UNPARSEABLE_MIME_TYPE.getIdentifier()));
  }

  @Test
  void decodeStringNullIfLengthOne() {
    ByteBuf fakeTypeBuffer = Unpooled.buffer(2);
    fakeTypeBuffer.writeByte(1);

    assertThatIllegalStateException()
        .isThrownBy(() -> decodeMimeTypeFromMimeBuffer(fakeTypeBuffer));
  }

  @Test
  void decodeStringNullIfLengthZero() {
    ByteBuf fakeTypeBuffer = Unpooled.buffer(2);

    assertThatIllegalStateException()
        .isThrownBy(() -> decodeMimeTypeFromMimeBuffer(fakeTypeBuffer));
  }

  @Test
  void decodeTypeSkipsFirstByte() {
    ByteBuf fakeTypeBuffer = Unpooled.buffer(2);
    fakeTypeBuffer.writeByte(128);
    fakeTypeBuffer.writeCharSequence("example", CharsetUtil.US_ASCII);

    assertThat(decodeMimeTypeFromMimeBuffer(fakeTypeBuffer)).hasToString("example");
  }

  @Test
  void encodeMetadataCustomTypeDelegates() {
    ByteBuf expected = CompositeMetadataCodec.encodeMetadataHeader(testAllocator, "foo", 2);

    CompositeByteBuf test = testAllocator.compositeBuffer();

    CompositeMetadataCodec.encodeAndAddMetadata(
        test, testAllocator, "foo", ByteBufUtils.getRandomByteBuf(2));

    assertThat((Iterable<? extends ByteBuf>) test).hasSize(2).first().isEqualTo(expected);
    test.release();
    expected.release();
  }

  @Test
  void encodeMetadataKnownTypeDelegates() {
    ByteBuf expected =
        CompositeMetadataCodec.encodeMetadataHeader(
            testAllocator, WellKnownMimeType.APPLICATION_OCTET_STREAM.getIdentifier(), 2);

    CompositeByteBuf test = testAllocator.compositeBuffer();

    CompositeMetadataCodec.encodeAndAddMetadata(
        test,
        testAllocator,
        WellKnownMimeType.APPLICATION_OCTET_STREAM,
        ByteBufUtils.getRandomByteBuf(2));

    assertThat((Iterable<? extends ByteBuf>) test).hasSize(2).first().isEqualTo(expected);
    test.release();
    expected.release();
  }

  @Test
  void encodeMetadataReservedTypeDelegates() {
    ByteBuf expected = CompositeMetadataCodec.encodeMetadataHeader(testAllocator, (byte) 120, 2);

    CompositeByteBuf test = testAllocator.compositeBuffer();

    CompositeMetadataCodec.encodeAndAddMetadata(
        test, testAllocator, (byte) 120, ByteBufUtils.getRandomByteBuf(2));

    assertThat((Iterable<? extends ByteBuf>) test).hasSize(2).first().isEqualTo(expected);
    test.release();
    expected.release();
  }

  @Test
  void encodeTryCompressWithCompressableType() {
    ByteBuf metadata = ByteBufUtils.getRandomByteBuf(2);
    CompositeByteBuf target = testAllocator.compositeBuffer();

    CompositeMetadataCodec.encodeAndAddMetadataWithCompression(
        target, testAllocator, WellKnownMimeType.APPLICATION_AVRO.getString(), metadata);

    assertThat(target.readableBytes()).as("readableBytes 1 + 3 + 2").isEqualTo(6);
    target.release();
  }

  @Test
  void encodeTryCompressWithCustomType() {
    ByteBuf metadata = ByteBufUtils.getRandomByteBuf(2);
    CompositeByteBuf target = testAllocator.compositeBuffer();

    CompositeMetadataCodec.encodeAndAddMetadataWithCompression(
        target, testAllocator, "custom/example", metadata);

    assertThat(target.readableBytes()).as("readableBytes 1 + 14 + 3 + 2").isEqualTo(20);
    target.release();
  }

  @Test
  void hasEntry() {
    WellKnownMimeType mime = WellKnownMimeType.APPLICATION_AVRO;

    CompositeByteBuf buffer =
        testAllocator
            .compositeBuffer()
            .addComponent(
                true,
                CompositeMetadataCodec.encodeMetadataHeader(testAllocator, mime.getIdentifier(), 0))
            .addComponent(
                true,
                CompositeMetadataCodec.encodeMetadataHeader(
                    testAllocator, mime.getIdentifier(), 0));

    assertThat(CompositeMetadataCodec.hasEntry(buffer, 0)).isTrue();
    assertThat(CompositeMetadataCodec.hasEntry(buffer, 4)).isTrue();
    assertThat(CompositeMetadataCodec.hasEntry(buffer, 8)).isFalse();
    buffer.release();
  }

  @Test
  void isWellKnownMimeType() {
    ByteBuf wellKnown = Unpooled.buffer().writeByte(0);
    assertThat(CompositeMetadataCodec.isWellKnownMimeType(wellKnown)).isTrue();

    ByteBuf explicit = Unpooled.buffer().writeByte(2).writeChar('a');
    assertThat(CompositeMetadataCodec.isWellKnownMimeType(explicit)).isFalse();
  }

  @Test
  void knownMimeHeader120_reserved() {
    byte mime = (byte) 120;
    ByteBuf encoded = CompositeMetadataCodec.encodeMetadataHeader(testAllocator, mime, 0);

    assertThat(mime)
        .as("smoke test RESERVED_120 unsigned 7 bits representation")
        .isEqualTo((byte) 0b01111000);

    assertThat(toHeaderBits(encoded)).startsWith("1").isEqualTo("11111000");

    final ByteBuf[] byteBufs = decodeMimeAndContentBuffersSlices(encoded, 0, false);
    assertThat(byteBufs).hasSize(2).doesNotContainNull();

    ByteBuf header = byteBufs[0];
    ByteBuf content = byteBufs[1];
    header.markReaderIndex();

    assertThat(header.readableBytes()).as("metadata header size").isOne();

    assertThat(byteToBitsString(header.readByte()))
        .as("header bit representation")
        .isEqualTo("11111000");

    header.resetReaderIndex();
    assertThat(decodeMimeIdFromMimeBuffer(header)).as("decoded mime id").isEqualTo(mime);

    assertThat(content.readableBytes()).as("no metadata content").isZero();
    encoded.release();
  }

  @Test
  void knownMimeHeader127_compositeMetadata() {
    WellKnownMimeType mime = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA;
    assertThat(mime.getIdentifier())
        .as("smoke test COMPOSITE unsigned 7 bits representation")
        .isEqualTo((byte) 127)
        .isEqualTo((byte) 0b01111111);
    ByteBuf encoded =
        CompositeMetadataCodec.encodeMetadataHeader(testAllocator, mime.getIdentifier(), 0);

    assertThat(toHeaderBits(encoded))
        .startsWith("1")
        .isEqualTo("11111111")
        .isEqualTo(byteToBitsString(mime.getIdentifier()).replaceFirst("0", "1"));

    final ByteBuf[] byteBufs = decodeMimeAndContentBuffersSlices(encoded, 0, false);
    assertThat(byteBufs).hasSize(2).doesNotContainNull();

    ByteBuf header = byteBufs[0];
    ByteBuf content = byteBufs[1];
    header.markReaderIndex();

    assertThat(header.readableBytes()).as("metadata header size").isOne();

    assertThat(byteToBitsString(header.readByte()))
        .as("header bit representation")
        .isEqualTo("11111111");

    header.resetReaderIndex();
    assertThat(decodeMimeIdFromMimeBuffer(header))
        .as("decoded mime id")
        .isEqualTo(mime.getIdentifier());

    assertThat(content.readableBytes()).as("no metadata content").isZero();
    encoded.release();
  }

  @Test
  void knownMimeHeaderZero_avro() {
    WellKnownMimeType mime = WellKnownMimeType.APPLICATION_AVRO;
    assertThat(mime.getIdentifier())
        .as("smoke test AVRO unsigned 7 bits representation")
        .isEqualTo((byte) 0)
        .isEqualTo((byte) 0b00000000);
    ByteBuf encoded =
        CompositeMetadataCodec.encodeMetadataHeader(testAllocator, mime.getIdentifier(), 0);

    assertThat(toHeaderBits(encoded))
        .startsWith("1")
        .isEqualTo("10000000")
        .isEqualTo(byteToBitsString(mime.getIdentifier()).replaceFirst("0", "1"));

    final ByteBuf[] byteBufs = decodeMimeAndContentBuffersSlices(encoded, 0, false);
    assertThat(byteBufs).hasSize(2).doesNotContainNull();

    ByteBuf header = byteBufs[0];
    ByteBuf content = byteBufs[1];
    header.markReaderIndex();

    assertThat(header.readableBytes()).as("metadata header size").isOne();

    assertThat(byteToBitsString(header.readByte()))
        .as("header bit representation")
        .isEqualTo("10000000");

    header.resetReaderIndex();
    assertThat(decodeMimeIdFromMimeBuffer(header))
        .as("decoded mime id")
        .isEqualTo(mime.getIdentifier());

    assertThat(content.readableBytes()).as("no metadata content").isZero();
    encoded.release();
  }

  @Test
  void encodeCustomHeaderAsciiCheckSkipsFirstByte() {
    final ByteBuf badBuf = Unpooled.copiedBuffer("é00000000000", CharsetUtil.UTF_8);
    badBuf.writerIndex(0);
    assertThat(badBuf.readerIndex()).isZero();

    ByteBufAllocator allocator =
        new AbstractByteBufAllocator() {
          @Override
          public boolean isDirectBufferPooled() {
            return false;
          }

          @Override
          protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
            return badBuf;
          }

          @Override
          protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
            return badBuf;
          }
        };

    assertThatCode(() -> CompositeMetadataCodec.encodeMetadataHeader(allocator, "custom/type", 0))
        .doesNotThrowAnyException();

    assertThat(badBuf.readByte()).isEqualTo((byte) 10);
    assertThat(badBuf.readCharSequence(11, CharsetUtil.UTF_8)).hasToString("custom/type");
    assertThat(badBuf.readUnsignedMedium()).isEqualTo(0);
  }
}
