package io.rsocket.metadata;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.CharsetUtil;
import io.rsocket.test.util.ByteBufUtils;
import io.rsocket.util.NumberUtils;
import org.junit.jupiter.api.Test;

import static io.rsocket.metadata.CompositeMetadataFlyweight.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class CompositeMetadataFlyweightTest {

    static String toHeaderBits(ByteBuf encoded) {
        encoded.markReaderIndex();
        byte headerByte = encoded.readByte();
        String byteAsString = byteToBitsString(headerByte);
        encoded.resetReaderIndex();
        return byteAsString;
    }

    static String byteToBitsString(byte b) {
        return String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0');
    }
    // ====

    @Test
    void knownMimeHeaderZero_avro() {
        WellKnownMimeType mime = WellKnownMimeType.APPLICATION_AVRO;
        assertThat(mime.getIdentifier())
                .as("smoke test AVRO unsigned 7 bits representation")
                .isEqualTo((byte) 0)
                .isEqualTo((byte) 0b00000000);
        ByteBuf encoded = CompositeMetadataFlyweight.encodeMetadataHeader(ByteBufAllocator.DEFAULT, mime.getIdentifier(), 0);

        assertThat(toHeaderBits(encoded))
                .startsWith("1")
                .isEqualTo("10000000")
                .isEqualTo(byteToBitsString(mime.getIdentifier()).replaceFirst("0", "1"));

        final ByteBuf[] byteBufs = decodeMimeAndContentBuffers(encoded, false);
        assertThat(byteBufs)
                .hasSize(2)
                .doesNotContainNull();

        ByteBuf header = byteBufs[0];
        ByteBuf content = byteBufs[1];
        header.markReaderIndex();

        assertThat(header.readableBytes())
                .as("metadata header size")
                .isOne();

        assertThat(byteToBitsString(header.readByte()))
                .as("header bit representation")
                .isEqualTo("10000000");

        header.resetReaderIndex();
        assertThat(decodeMimeIdFromMimeBuffer(header))
                .as("decoded mime id")
                .isEqualTo(mime.getIdentifier());

        assertThat(content.readableBytes())
                .as("no metadata content")
                .isZero();
    }

    @Test
    void knownMimeHeader127_compositeMetadata() {
        WellKnownMimeType mime = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA;
        assertThat(mime.getIdentifier())
                .as("smoke test COMPOSITE unsigned 7 bits representation")
                .isEqualTo((byte) 127)
                .isEqualTo((byte) 0b01111111);
        ByteBuf encoded = CompositeMetadataFlyweight.encodeMetadataHeader(ByteBufAllocator.DEFAULT, mime.getIdentifier(), 0);

        assertThat(toHeaderBits(encoded))
                .startsWith("1")
                .isEqualTo("11111111")
                .isEqualTo(byteToBitsString(mime.getIdentifier()).replaceFirst("0", "1"));

        final ByteBuf[] byteBufs = decodeMimeAndContentBuffers(encoded, false);
        assertThat(byteBufs)
                .hasSize(2)
                .doesNotContainNull();

        ByteBuf header = byteBufs[0];
        ByteBuf content = byteBufs[1];
        header.markReaderIndex();

        assertThat(header.readableBytes())
                .as("metadata header size")
                .isOne();

        assertThat(byteToBitsString(header.readByte()))
                .as("header bit representation")
                .isEqualTo("11111111");

        header.resetReaderIndex();
        assertThat(decodeMimeIdFromMimeBuffer(header))
                .as("decoded mime id")
                .isEqualTo(mime.getIdentifier());

        assertThat(content.readableBytes())
                .as("no metadata content")
                .isZero();
    }

    @Test
    void knownMimeHeader120_reserved() {
        byte mime = (byte) 120;
        ByteBuf encoded = CompositeMetadataFlyweight.encodeMetadataHeader(ByteBufAllocator.DEFAULT, mime, 0);

        assertThat(mime).as("smoke test RESERVED_120 unsigned 7 bits representation")
                .isEqualTo((byte) 0b01111000);

        assertThat(toHeaderBits(encoded))
                .startsWith("1")
                .isEqualTo("11111000");

        final ByteBuf[] byteBufs = decodeMimeAndContentBuffers(encoded, false);
        assertThat(byteBufs)
                .hasSize(2)
                .doesNotContainNull();

        ByteBuf header = byteBufs[0];
        ByteBuf content = byteBufs[1];
        header.markReaderIndex();

        assertThat(header.readableBytes())
                .as("metadata header size")
                .isOne();

        assertThat(byteToBitsString(header.readByte()))
                .as("header bit representation")
                .isEqualTo("11111000");

        header.resetReaderIndex();
        assertThat(decodeMimeIdFromMimeBuffer(header))
                .as("decoded mime id")
                .isEqualTo(mime);

        assertThat(content.readableBytes())
                .as("no metadata content")
                .isZero();
    }

    @Test
    void customMimeHeaderLengthOne() {
        String mimeString ="w";
        ByteBuf encoded = CompositeMetadataFlyweight.encodeMetadataHeader(ByteBufAllocator.DEFAULT, mimeString, 0);

        assertThat(toHeaderBits(encoded))
                .startsWith("0")
                .isEqualTo("00000000");

        final ByteBuf[] byteBufs = decodeMimeAndContentBuffers(encoded, false);
        assertThat(byteBufs)
                .hasSize(2)
                .doesNotContainNull();

        ByteBuf header = byteBufs[0];
        ByteBuf content = byteBufs[1];
        header.markReaderIndex();

        assertThat(header.readableBytes())
                .as("metadata header size")
                .isGreaterThan(1);

        assertThat((int) header.readByte())
                .as("mime length")
                .isEqualTo(1 -1); //encoded as actual length - 1

        assertThat(header.readCharSequence(1, CharsetUtil.US_ASCII))
                .as("mime string")
                .hasToString(mimeString);

        header.resetReaderIndex();
        assertThat(CompositeMetadataFlyweight.decodeMimeTypeFromMimeBuffer(header))
                .as("decoded mime string")
                .hasToString(mimeString);

        assertThat(content.readableBytes())
                .as("no metadata content")
                .isZero();
    }

    @Test
    void customMimeHeaderLengthTwo() {
        String mimeString ="ww";
        ByteBuf encoded = CompositeMetadataFlyweight.encodeMetadataHeader(ByteBufAllocator.DEFAULT, mimeString, 0);

        assertThat(toHeaderBits(encoded))
                .startsWith("0")
                .isEqualTo("00000001");

        final ByteBuf[] byteBufs = decodeMimeAndContentBuffers(encoded, false);
        assertThat(byteBufs)
                .hasSize(2)
                .doesNotContainNull();

        ByteBuf header = byteBufs[0];
        ByteBuf content = byteBufs[1];
        header.markReaderIndex();

        assertThat(header.readableBytes())
                .as("metadata header size")
                .isGreaterThan(1);

        assertThat((int) header.readByte())
                .as("mime length")
                .isEqualTo(2 - 1); //encoded as actual length - 1

        assertThat(header.readCharSequence(2, CharsetUtil.US_ASCII))
                .as("mime string")
                .hasToString(mimeString);

        header.resetReaderIndex();
        assertThat(CompositeMetadataFlyweight.decodeMimeTypeFromMimeBuffer(header))
                .as("decoded mime string")
                .hasToString(mimeString);

        assertThat(content.readableBytes())
                .as("no metadata content")
                .isZero();
    }

    @Test
    void customMimeHeaderLength127() {
        StringBuilder builder = new StringBuilder(127);
        for (int i = 0; i < 127; i++) {
            builder.append('a');
        }
        String mimeString = builder.toString();
        ByteBuf encoded = CompositeMetadataFlyweight.encodeMetadataHeader(ByteBufAllocator.DEFAULT, mimeString, 0);

        assertThat(toHeaderBits(encoded))
                .startsWith("0")
                .isEqualTo("01111110");

        final ByteBuf[] byteBufs = decodeMimeAndContentBuffers(encoded, false);
        assertThat(byteBufs)
                .hasSize(2)
                .doesNotContainNull();

        ByteBuf header = byteBufs[0];
        ByteBuf content = byteBufs[1];
        header.markReaderIndex();

        assertThat(header.readableBytes())
                .as("metadata header size")
                .isGreaterThan(1);

        assertThat((int) header.readByte())
                .as("mime length")
                .isEqualTo(127 - 1); //encoded as actual length - 1

        assertThat(header.readCharSequence(127, CharsetUtil.US_ASCII))
                .as("mime string")
                .hasToString(mimeString);

        header.resetReaderIndex();
        assertThat(CompositeMetadataFlyweight.decodeMimeTypeFromMimeBuffer(header))
                .as("decoded mime string")
                .hasToString(mimeString);

        assertThat(content.readableBytes())
                .as("no metadata content")
                .isZero();
    }

    @Test
    void customMimeHeaderLength128() {
        StringBuilder builder = new StringBuilder(128);
        for (int i = 0; i < 128; i++) {
            builder.append('a');
        }
        String mimeString = builder.toString();
        ByteBuf encoded = CompositeMetadataFlyweight.encodeMetadataHeader(ByteBufAllocator.DEFAULT, mimeString, 0);

        assertThat(toHeaderBits(encoded))
                .startsWith("0")
                .isEqualTo("01111111");

        final ByteBuf[] byteBufs = decodeMimeAndContentBuffers(encoded, false);
        assertThat(byteBufs)
                .hasSize(2)
                .doesNotContainNull();

        ByteBuf header = byteBufs[0];
        ByteBuf content = byteBufs[1];
        header.markReaderIndex();

        assertThat(header.readableBytes())
                .as("metadata header size")
                .isGreaterThan(1);

        assertThat((int) header.readByte())
                .as("mime length")
                .isEqualTo(128 - 1); //encoded as actual length - 1

        assertThat(header.readCharSequence(128, CharsetUtil.US_ASCII))
                .as("mime string")
                .hasToString(mimeString);

        header.resetReaderIndex();
        assertThat(CompositeMetadataFlyweight.decodeMimeTypeFromMimeBuffer(header))
                .as("decoded mime string")
                .hasToString(mimeString);

        assertThat(content.readableBytes())
                .as("no metadata content")
                .isZero();
    }

    @Test
    void customMimeHeaderLength129_encodingFails() {
        StringBuilder builder = new StringBuilder(129);
        for (int i = 0; i < 129; i++) {
            builder.append('a');
        }

        assertThatIllegalArgumentException()
                .isThrownBy(() -> CompositeMetadataFlyweight.encodeMetadataHeader(ByteBufAllocator.DEFAULT, builder.toString(), 0))
                .withMessage("custom mime type must have a strictly positive length that fits on 7 unsigned bits, ie 1-128");
    }

    @Test
    void customMimeHeaderNonAscii_encodingFails() {
        String mimeNotAscii = "mime/typé";

        assertThatIllegalArgumentException()
                .isThrownBy(() -> CompositeMetadataFlyweight.encodeMetadataHeader(ByteBufAllocator.DEFAULT, mimeNotAscii, 0))
                .withMessage("custom mime type must be US_ASCII characters only");
    }

    @Test
    void customMimeHeaderLength0_encodingFails() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> CompositeMetadataFlyweight.encodeMetadataHeader(ByteBufAllocator.DEFAULT, "", 0))
                .withMessage("custom mime type must have a strictly positive length that fits on 7 unsigned bits, ie 1-128");
    }

    @Test
    void decodeEntryTooShortForMimeLength() {
        ByteBuf fakeEntry = ByteBufAllocator.DEFAULT.buffer();
        fakeEntry.writeByte(120);

        assertThat(decodeMimeAndContentBuffers(fakeEntry, false))
                .isSameAs(METADATA_MALFORMED);
    }

    @Test
    void decodeEntryHasNoContentLength() {
        ByteBuf fakeEntry = ByteBufAllocator.DEFAULT.buffer();
        fakeEntry.writeByte(0);
        fakeEntry.writeCharSequence("w", CharsetUtil.US_ASCII);

        assertThat(decodeMimeAndContentBuffers(fakeEntry, false))
                .isSameAs(METADATA_MALFORMED);
    }

    @Test
    void decodeEntryTooShortForContentLength() {
        ByteBuf fakeEntry = ByteBufAllocator.DEFAULT.buffer();
        fakeEntry.writeByte(1);
        fakeEntry.writeCharSequence("w", CharsetUtil.US_ASCII);
        NumberUtils.encodeUnsignedMedium(fakeEntry, 456);
        fakeEntry.writeChar('w');

        assertThat(decodeMimeAndContentBuffers(fakeEntry, false))
                .isSameAs(METADATA_MALFORMED);
    }

    @Test
    void decodeEntryAtEndOfBuffer() {
        ByteBuf fakeEntry = ByteBufAllocator.DEFAULT.buffer();

        assertThat(decodeMimeAndContentBuffers(fakeEntry, false))
                .isSameAs(METADATA_BUFFERS_DONE);
    }

    @Test
    void decodeIdMinusTwoWhenZeroByte() {
        ByteBuf fakeIdBuffer = ByteBufAllocator.DEFAULT.buffer(0);

        assertThat(decodeMimeIdFromMimeBuffer(fakeIdBuffer))
                .isEqualTo((WellKnownMimeType.UNPARSEABLE_MIME_TYPE.getIdentifier()));
    }

    @Test
    void decodeIdMinusTwoWhenMoreThanOneByte() {
        ByteBuf fakeIdBuffer = ByteBufAllocator.DEFAULT.buffer(2);
        fakeIdBuffer.writeInt(200);

        assertThat(decodeMimeIdFromMimeBuffer(fakeIdBuffer))
                .isEqualTo((WellKnownMimeType.UNPARSEABLE_MIME_TYPE.getIdentifier()));
    }

    @Test
    void decodeStringNullIfLengthZero() {
        ByteBuf fakeTypeBuffer = ByteBufAllocator.DEFAULT.buffer(2);

        assertThat(decodeMimeTypeFromMimeBuffer(fakeTypeBuffer))
                .isNull();
    }

    @Test
    void decodeStringNullIfLengthOne() {
        ByteBuf fakeTypeBuffer = ByteBufAllocator.DEFAULT.buffer(2);
        fakeTypeBuffer.writeByte(1);

        assertThat(decodeMimeTypeFromMimeBuffer(fakeTypeBuffer))
                .isNull();
    }

    @Test
    void decodeTypeSkipsFirstByte() {
        ByteBuf fakeTypeBuffer = ByteBufAllocator.DEFAULT.buffer(2);
        fakeTypeBuffer.writeByte(128);
        fakeTypeBuffer.writeCharSequence("example", CharsetUtil.US_ASCII);

        assertThat(decodeMimeTypeFromMimeBuffer(fakeTypeBuffer))
                .hasToString("example");
    }

    @Test
    void encodeMetadataKnownTypeDelegates() {
        ByteBuf expected = CompositeMetadataFlyweight.encodeMetadataHeader(ByteBufAllocator.DEFAULT,
                WellKnownMimeType.APPLICATION_OCTET_STREAM.getIdentifier(),
                2);

        CompositeByteBuf test = ByteBufAllocator.DEFAULT.compositeBuffer();

        CompositeMetadataFlyweight.encodeAndAddMetadata(
                test, ByteBufAllocator.DEFAULT,
                WellKnownMimeType.APPLICATION_OCTET_STREAM,
                ByteBufUtils.getRandomByteBuf(2));

        assertThat((Iterable<? extends ByteBuf>) test)
                .hasSize(2)
                .first()
                .isEqualTo(expected);
    }

    @Test
    void encodeMetadataReservedTypeDelegates() {
        ByteBuf expected = CompositeMetadataFlyweight.encodeMetadataHeader(ByteBufAllocator.DEFAULT,
                (byte) 120,
                2);

        CompositeByteBuf test = ByteBufAllocator.DEFAULT.compositeBuffer();

        CompositeMetadataFlyweight.encodeAndAddMetadata(
                test, ByteBufAllocator.DEFAULT,
                (byte) 120,
                ByteBufUtils.getRandomByteBuf(2));

        assertThat((Iterable<? extends ByteBuf>) test)
                .hasSize(2)
                .first()
                .isEqualTo(expected);
    }

    @Test
    void encodeMetadataCustomTypeDelegates() {
        ByteBuf expected = CompositeMetadataFlyweight.encodeMetadataHeader(ByteBufAllocator.DEFAULT,
                "foo", 2);

        CompositeByteBuf test = ByteBufAllocator.DEFAULT.compositeBuffer();

        CompositeMetadataFlyweight.encodeAndAddMetadata(
                test, ByteBufAllocator.DEFAULT,
                "foo",
                ByteBufUtils.getRandomByteBuf(2));

        assertThat((Iterable<? extends ByteBuf>) test)
                .hasSize(2)
                .first()
                .isEqualTo(expected);
    }

//    @Test
//    void decodeMetadataLengthFromUntouchedWithKnownMime() {
//        ByteBuf encoded = CompositeMetadataFlyweight.encodeMetadataHeader(ByteBufAllocator.DEFAULT, WellKnownMimeType.APPLICATION_GZIP.getIdentifier(), 12);
//
//        assertThat(CompositeMetadataFlyweight.decodeMetadataLengthFromMetadataHeader(encoded))
//                .withFailMessage("should not correctly decode if not at correct reader index")
//                .isNotEqualTo(12);
//    }
//
//    @Test
//    void decodeMetadataLengthFromMimeDecodedWithKnownMime() {
//        ByteBuf encoded = CompositeMetadataFlyweight.encodeMetadataHeader(ByteBufAllocator.DEFAULT, WellKnownMimeType.APPLICATION_GZIP.getIdentifier(), 12);
//        CompositeMetadataFlyweight.decode3WaysMimeFromMetadataHeader(encoded);
//
//        assertThat(CompositeMetadataFlyweight.decodeMetadataLengthFromMetadataHeader(encoded)).isEqualTo(12);
//    }
//
//    @Test
//    void decodeMetadataLengthFromUntouchedWithCustomMime() {
//        ByteBuf encoded = CompositeMetadataFlyweight.encodeMetadataHeader(ByteBufAllocator.DEFAULT, "foo/bar", 12);
//
//        assertThat(CompositeMetadataFlyweight.decodeMetadataLengthFromMetadataHeader(encoded))
//                .withFailMessage("should not correctly decode if not at correct reader index")
//                .isNotEqualTo(12);
//    }
//
//    @Test
//    void decodeMetadataLengthFromMimeDecodedWithCustomMime() {
//        ByteBuf encoded = CompositeMetadataFlyweight.encodeMetadataHeader(ByteBufAllocator.DEFAULT, "foo/bar", 12);
//        CompositeMetadataFlyweight.decode3WaysMimeFromMetadataHeader(encoded);
//
//        assertThat(CompositeMetadataFlyweight.decodeMetadataLengthFromMetadataHeader(encoded)).isEqualTo(12);
//    }
//
//    @Test
//    void decodeMetadataLengthFromTooShortBuffer() {
//        ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
//        buffer.writeShort(12);
//
//        assertThatExceptionOfType(RuntimeException.class)
//                .isThrownBy(() -> CompositeMetadataFlyweight.decodeMetadataLengthFromMetadataHeader(buffer))
//                .withMessage("the given buffer should contain at least 3 readable bytes after decoding mime type");
//    }


}