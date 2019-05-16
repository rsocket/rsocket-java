package io.rsocket.metadata;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class CompositeMetadataFlyweightTest {

    static String toHeaderBits(ByteBuf encoded) {
        encoded.markReaderIndex();
        byte headerByte = encoded.readByte();
        String byteAsString = String.format("%8s", Integer.toBinaryString(headerByte & 0xFF)).replace(' ', '0');
        encoded.resetReaderIndex();
        return byteAsString;
    }
    // ====

    @Test
    void knownMimeHeaderZero_avro() {
        WellKnownMimeType mime = WellKnownMimeType.APPLICATION_AVRO;
        assertThat(mime.getIdentifier()).as("AVRO identifier").isZero();
        ByteBuf encoded = CompositeMetadataFlyweight.encodeMetadataHeader(ByteBufAllocator.DEFAULT, mime, 0);

        assertThat(toHeaderBits(encoded))
                .startsWith("1")
                .isEqualTo("10000000");

        String decoded = CompositeMetadataFlyweight.decodeMimeFromMetadataHeader(encoded);

        assertThat(decoded).isEqualTo(mime.toString());
    }

    @Test
    void knownMimeHeader127_compositeMetadata() {
        WellKnownMimeType mime = WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA;
        assertThat(mime.getIdentifier()).as("COMPOSITE METADATA identifier").isEqualTo((byte) 127);
        ByteBuf encoded = CompositeMetadataFlyweight.encodeMetadataHeader(ByteBufAllocator.DEFAULT, mime, 0);

        assertThat(toHeaderBits(encoded))
                .startsWith("1")
                .isEqualTo("11111111");

        String decoded = CompositeMetadataFlyweight.decodeMimeFromMetadataHeader(encoded);

        assertThat(decoded).isEqualTo(mime.toString());
    }

    @Test
    void customMimeHeaderLengthOne() {
        String mimeString ="w";
        ByteBuf encoded = CompositeMetadataFlyweight.encodeMetadataHeader(ByteBufAllocator.DEFAULT, mimeString, 0);

        assertThat(toHeaderBits(encoded))
                .startsWith("0")
                .isEqualTo("00000000");

        String decoded = CompositeMetadataFlyweight.decodeMimeFromMetadataHeader(encoded);

        assertThat(decoded).isEqualTo(mimeString);
    }

    @Test
    void customMimeHeaderLength127() {
        StringBuilder builder = new StringBuilder(127);
        for (int i = 0; i < 127; i++) {
            builder.append('a');
        }
        String longString = builder.toString();
        ByteBuf encoded = CompositeMetadataFlyweight.encodeMetadataHeader(ByteBufAllocator.DEFAULT, longString, 0);

        assertThat(toHeaderBits(encoded))
                .startsWith("0")
                .isEqualTo("01111110");

        String decoded = CompositeMetadataFlyweight.decodeMimeFromMetadataHeader(encoded);

        assertThat(decoded).isEqualTo(longString);
    }

    @Test
    void customMimeHeaderLength128() {
        StringBuilder builder = new StringBuilder(128);
        for (int i = 0; i < 128; i++) {
            builder.append('a');
        }
        String longString = builder.toString();
        ByteBuf encoded = CompositeMetadataFlyweight.encodeMetadataHeader(ByteBufAllocator.DEFAULT, longString, 0);

        assertThat(toHeaderBits(encoded))
                .startsWith("0")
                .isEqualTo("01111111");

        String decoded = CompositeMetadataFlyweight.decodeMimeFromMetadataHeader(encoded);

        assertThat(decoded).isEqualTo(longString);
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
    void decodeMetadataLengthFromUntouchedWithKnownMime() {
        ByteBuf encoded = CompositeMetadataFlyweight.encodeMetadataHeader(ByteBufAllocator.DEFAULT, WellKnownMimeType.APPLICATION_GZIP, 12);

        assertThat(CompositeMetadataFlyweight.decodeMetadataLengthFromMetadataHeader(encoded))
                .withFailMessage("should not correctly decode if not at correct reader index")
                .isNotEqualTo(12);
    }

    @Test
    void decodeMetadataLengthFromMimeDecodedWithKnownMime() {
        ByteBuf encoded = CompositeMetadataFlyweight.encodeMetadataHeader(ByteBufAllocator.DEFAULT, WellKnownMimeType.APPLICATION_GZIP, 12);
        CompositeMetadataFlyweight.decodeMimeFromMetadataHeader(encoded);

        assertThat(CompositeMetadataFlyweight.decodeMetadataLengthFromMetadataHeader(encoded)).isEqualTo(12);
    }

    @Test
    void decodeMetadataLengthFromUntouchedWithCustomMime() {
        ByteBuf encoded = CompositeMetadataFlyweight.encodeMetadataHeader(ByteBufAllocator.DEFAULT, "foo/bar", 12);

        assertThat(CompositeMetadataFlyweight.decodeMetadataLengthFromMetadataHeader(encoded))
                .withFailMessage("should not correctly decode if not at correct reader index")
                .isNotEqualTo(12);
    }

    @Test
    void decodeMetadataLengthFromMimeDecodedWithCustomMime() {
        ByteBuf encoded = CompositeMetadataFlyweight.encodeMetadataHeader(ByteBufAllocator.DEFAULT, "foo/bar", 12);
        CompositeMetadataFlyweight.decodeMimeFromMetadataHeader(encoded);

        assertThat(CompositeMetadataFlyweight.decodeMetadataLengthFromMetadataHeader(encoded)).isEqualTo(12);
    }

    @Test
    void compositeMetadata() {
        //metadata 1:
        WellKnownMimeType mimeType1 = WellKnownMimeType.APPLICATION_PDF;
        ByteBuf metadata1 = ByteBufAllocator.DEFAULT.buffer();
        metadata1.writeCharSequence("abcdefghijkl", CharsetUtil.UTF_8);
        int metadataLength1 = metadata1.readableBytes();

        //metadata 2:
        String mimeType2 = "application/custom";
        ByteBuf metadata2 = ByteBufAllocator.DEFAULT.buffer();
        metadata2.writeChar('E');
        metadata2.writeChar('∑');
        metadata2.writeChar('é');
        metadata2.writeBoolean(true);
        metadata2.writeChar('W');
        int metadataLength2 = metadata2.readableBytes();

        CompositeByteBuf compositeMetadata = ByteBufAllocator.DEFAULT.compositeBuffer();
        CompositeMetadataFlyweight.addMetadata(compositeMetadata, ByteBufAllocator.DEFAULT, mimeType1, metadata1);
        CompositeMetadataFlyweight.addMetadata(compositeMetadata, ByteBufAllocator.DEFAULT, mimeType2, metadata2);
        System.out.println(ByteBufUtil.prettyHexDump(compositeMetadata));

        compositeMetadata.readByte(); //ignore the "know mime + ID" byte for now
        assertThat(compositeMetadata.readUnsignedMedium())
                .as("metadata1 length")
                .isEqualTo(metadataLength1);
        assertThat(compositeMetadata.readCharSequence(metadataLength1, CharsetUtil.UTF_8))
                .as("metadata1 value").isEqualTo("abcdefghijkl");

        int mimeLength = compositeMetadata.readByte() + 1;

        assertThat(compositeMetadata.readCharSequence(mimeLength, CharsetUtil.US_ASCII).toString())
                .as("metadata2 custom mime ")
                .isEqualTo(mimeType2);
        assertThat(compositeMetadata.readUnsignedMedium())
                .as("metadata2 length")
                .isEqualTo(metadataLength2);
        assertThat(compositeMetadata.readChar())
                .as("metadata2 value 1/5")
                .isEqualTo('E');
        assertThat(compositeMetadata.readChar())
                .as("metadata2 value 2/5")
                .isEqualTo('∑');

        assertThat(compositeMetadata.readChar())
                .as("metadata2 value 3/5")
                .isEqualTo('é');
        assertThat(compositeMetadata.readBoolean())
                .as("metadata2 value 4/5")
                .isTrue();
        assertThat(compositeMetadata.readChar())
                .as("metadata2 value 5/5")
                .isEqualTo('W');

        assertThat(compositeMetadata.readableBytes())
                .as("reading composite metadata done")
                .isZero();
    }

    @Test
    void decodeCompositeMetadata() {
        //metadata 1:
        WellKnownMimeType mimeType1 = WellKnownMimeType.APPLICATION_PDF;
        ByteBuf metadata1 = ByteBufAllocator.DEFAULT.buffer();
        metadata1.writeCharSequence("abcdefghijkl", CharsetUtil.UTF_8);

        //metadata 2:
        String mimeType2 = "application/custom";
        ByteBuf metadata2 = ByteBufAllocator.DEFAULT.buffer();
        metadata2.writeChar('E');
        metadata2.writeChar('∑');
        metadata2.writeChar('é');
        metadata2.writeBoolean(true);
        metadata2.writeChar('W');

        CompositeByteBuf compositeMetadata = ByteBufAllocator.DEFAULT.compositeBuffer();
        CompositeMetadataFlyweight.addMetadata(compositeMetadata, ByteBufAllocator.DEFAULT, mimeType1, metadata1);
        CompositeMetadataFlyweight.addMetadata(compositeMetadata, ByteBufAllocator.DEFAULT, mimeType2, metadata2);

        Object[] decoded = CompositeMetadataFlyweight.decodeNext(compositeMetadata, false);
        assertThat(decoded).as("first decode").hasSize(2);

        assertThat(decoded[0])
                .as("first mime")
                .isInstanceOf(String.class)
                .isEqualTo(WellKnownMimeType.APPLICATION_PDF.getMime());

        assertThat((ByteBuf) decoded[1])
                .as("first content")
                .isEqualByComparingTo(metadata1)
                .extracting(o -> o.toString(CharsetUtil.UTF_8))
                .isEqualTo("abcdefghijkl");


        decoded = CompositeMetadataFlyweight.decodeNext(compositeMetadata, false);

        assertThat(decoded).as("second decode").hasSize(2);

        assertThat(decoded[0])
                .as("second mime")
                .isInstanceOf(String.class)
                .isEqualTo("application/custom");

        assertThat(decoded[1]).isInstanceOf(ByteBuf.class);
        ByteBuf secondBuffer = (ByteBuf) decoded[1];
        System.out.println(ByteBufUtil.hexDump(secondBuffer));

        assertThat(secondBuffer)
                .as("second content")
                .isEqualByComparingTo(metadata2);

        assertThat(CompositeMetadataFlyweight.decodeNext(compositeMetadata, false)).isEmpty();
    }

    @Test
    void decodeCompositeMetadataRetainSlices() {
        //metadata 1:
        WellKnownMimeType mimeType1 = WellKnownMimeType.APPLICATION_PDF;
        ByteBuf metadata1 = ByteBufAllocator.DEFAULT.buffer();
        metadata1.writeCharSequence("abcdefghijkl", CharsetUtil.UTF_8);

        //metadata 2:
        String mimeType2 = "application/custom";
        ByteBuf metadata2 = ByteBufAllocator.DEFAULT.buffer();
        metadata2.writeChar('E');
        metadata2.writeChar('∑');
        metadata2.writeChar('é');
        metadata2.writeBoolean(true);
        metadata2.writeChar('W');

        CompositeByteBuf compositeMetadata = ByteBufAllocator.DEFAULT.compositeBuffer();
        CompositeMetadataFlyweight.addMetadata(compositeMetadata, ByteBufAllocator.DEFAULT, mimeType1, metadata1);
        CompositeMetadataFlyweight.addMetadata(compositeMetadata, ByteBufAllocator.DEFAULT, mimeType2, metadata2);

        List<ByteBuf> bufs = new ArrayList<>();
        Object[] decoded;
        do {
            decoded = CompositeMetadataFlyweight.decodeNext(compositeMetadata, true);
            if (decoded.length == 2) {
                bufs.add((ByteBuf) decoded[1]);
            }
        } while(decoded.length > 0);

        assertThat(bufs)
                .as("metadata buffers retained")
                .allSatisfy(buf -> assertThat(buf.refCnt())
                        .isGreaterThan(1));
    }

    @Test
    void decodeCompositeMetadataNoRetainSlices() {
        //metadata 1:
        WellKnownMimeType mimeType1 = WellKnownMimeType.APPLICATION_PDF;
        ByteBuf metadata1 = ByteBufAllocator.DEFAULT.buffer();
        metadata1.writeCharSequence("abcdefghijkl", CharsetUtil.UTF_8);

        //metadata 2:
        String mimeType2 = "application/custom";
        ByteBuf metadata2 = ByteBufAllocator.DEFAULT.buffer();
        metadata2.writeChar('E');
        metadata2.writeChar('∑');
        metadata2.writeChar('é');
        metadata2.writeBoolean(true);
        metadata2.writeChar('W');

        CompositeByteBuf compositeMetadata = ByteBufAllocator.DEFAULT.compositeBuffer();
        CompositeMetadataFlyweight.addMetadata(compositeMetadata, ByteBufAllocator.DEFAULT, mimeType1, metadata1);
        CompositeMetadataFlyweight.addMetadata(compositeMetadata, ByteBufAllocator.DEFAULT, mimeType2, metadata2);

        List<ByteBuf> bufs = new ArrayList<>();
        Object[] decoded;
        do {
            decoded = CompositeMetadataFlyweight.decodeNext(compositeMetadata, false);
            if (decoded.length == 2) {
                bufs.add((ByteBuf) decoded[1]);
            }
        } while(decoded.length > 0);

        assertThat(bufs)
                .as("metadata buffers not retained")
                .allSatisfy(buf -> assertThat(buf.refCnt())
                        .isOne());
    }

}