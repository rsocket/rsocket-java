package io.rsocket.metadata;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.CharsetUtil;
import io.rsocket.metadata.CompositeMetadata.CompressedTypeEntry;
import io.rsocket.metadata.CompositeMetadata.CustomTypeEntry;
import io.rsocket.metadata.CompositeMetadata.Entry;
import io.rsocket.metadata.CompositeMetadata.UnknownCompressedTypeEntry;
import io.rsocket.test.util.ByteBufUtils;
import io.rsocket.util.NumberUtils;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static io.netty.util.CharsetUtil.UTF_8;
import static org.assertj.core.api.Assertions.*;

class CompositeMetadataTest {

    @Test
    void decodeCompositeMetadata() {
        //metadata 1: well known
        WellKnownMimeType mimeType1 = WellKnownMimeType.APPLICATION_PDF;
        ByteBuf metadata1 = ByteBufAllocator.DEFAULT.buffer();
        metadata1.writeCharSequence("abcdefghijkl", UTF_8);

        //metadata 2: custom
        String mimeType2 = "application/custom";
        ByteBuf metadata2 = ByteBufAllocator.DEFAULT.buffer();
        metadata2.writeChar('E');
        metadata2.writeChar('∑');
        metadata2.writeChar('é');
        metadata2.writeBoolean(true);
        metadata2.writeChar('W');

        //metadata 3: reserved but unknown
        byte reserved = 120;
        assertThat(WellKnownMimeType.fromId(reserved))
                .as("ensure UNKNOWN RESERVED used in test")
                .isSameAs(WellKnownMimeType.UNKNOWN_RESERVED_MIME_TYPE);
        ByteBuf metadata3 = ByteBufAllocator.DEFAULT.buffer();
        metadata3.writeByte(88);

        CompositeByteBuf compositeMetadata = ByteBufAllocator.DEFAULT.compositeBuffer();
        CompositeMetadataFlyweight.encodeAndAddMetadata(compositeMetadata, ByteBufAllocator.DEFAULT, mimeType1, metadata1);
        CompositeMetadataFlyweight.encodeAndAddMetadata(compositeMetadata, ByteBufAllocator.DEFAULT, mimeType2, metadata2);
        CompositeMetadataFlyweight.encodeAndAddMetadata(compositeMetadata, ByteBufAllocator.DEFAULT, reserved, metadata3);

        CompositeMetadata metadata = CompositeMetadata.decodeComposite(compositeMetadata);

        assertThat(metadata.size()).as("size").isEqualTo(3);

        assertThat(metadata.get(0))
                .satisfies(e -> assertThat(e.getMimeType())
                        .as("metadata1 mime")
                        .isEqualTo(WellKnownMimeType.APPLICATION_PDF.getMime())
                )
                .satisfies(e -> assertThat(e.getMetadata().toString(UTF_8))
                        .as("metadata1 decoded")
                        .isEqualTo("abcdefghijkl")
                );

        System.out.println(ByteBufUtil.hexDump(metadata.get(1).getMetadata()));

        assertThat(metadata.get(1))
                .satisfies(e -> assertThat(e.getMimeType())
                        .as("metadata2 mime")
                        .isEqualTo(mimeType2)
                )
                .satisfies(e -> assertThat(e.getMetadata())
                        .as("metadata2 buffer")
                        .isEqualByComparingTo(metadata2)
                );

        assertThat(metadata.get(2))
                .matches(Entry::isPassthrough)
                .isInstanceOf(UnknownCompressedTypeEntry.class)
                .satisfies(e -> assertThat(e.getMimeType()).isEqualTo(WellKnownMimeType.UNKNOWN_RESERVED_MIME_TYPE.getMime()))
                .satisfies(e -> assertThat(((UnknownCompressedTypeEntry) e).getUnknownReservedId())
                        .isEqualTo(reserved));
    }

    @Test
    void encodeEntryWellKnownMetadata() {
        WellKnownMimeType type = WellKnownMimeType.fromId(5);
        //5 = 0b00000101
        byte expected = (byte) 0b10000101;

        ByteBuf content = ByteBufUtils.getRandomByteBuf(2);
        Entry entry = new CompressedTypeEntry(type, content);

        final CompositeByteBuf metadata = ByteBufAllocator.DEFAULT.compositeBuffer();
        CompositeMetadata.encodeEntry(ByteBufAllocator.DEFAULT, metadata, entry);

        assertThat(metadata.readByte())
                .as("mime header")
                .isEqualTo(expected);
        assertThat(metadata.readUnsignedMedium()).as("length header").isEqualTo(2);
        assertThat(metadata.readSlice(2)).as("content").isEqualByComparingTo(content);
    }

    @Test
    void encodeEntryCustomMetadata() {
        // length 3, encoded as length - 1 since 0 is not authorized
        byte expected = (byte) 2;
        ByteBuf content = ByteBufUtils.getRandomByteBuf(2);
        Entry entry = new CustomTypeEntry("foo", content);

        final CompositeByteBuf metadata = ByteBufAllocator.DEFAULT.compositeBuffer();
        CompositeMetadata.encodeEntry(ByteBufAllocator.DEFAULT, metadata, entry);

        assertThat(metadata.readByte())
                .as("mime header")
                .isEqualTo(expected);
        assertThat(metadata.readCharSequence(3, CharsetUtil.US_ASCII).toString())
                .isEqualTo("foo");
        assertThat(metadata.readUnsignedMedium()).as("length header").isEqualTo(2);
        assertThat(metadata.readSlice(2)).as("content").isEqualByComparingTo(content);
    }

    @Test
    void encodeEntryPassthroughMetadata() {
        //120 = 0b01111000
        byte expected = (byte) 0b11111000;

        ByteBuf content = ByteBufUtils.getRandomByteBuf(2);
        UnknownCompressedTypeEntry entry = new UnknownCompressedTypeEntry((byte) 120, content);

        final CompositeByteBuf metadata = ByteBufAllocator.DEFAULT.compositeBuffer();
        CompositeMetadata.encodeEntry(ByteBufAllocator.DEFAULT, metadata, entry);

        assertThat(metadata.readByte())
                .as("mime header")
                .isEqualTo(expected);
        assertThat(metadata.readUnsignedMedium()).as("length header").isEqualTo(2);
        assertThat(metadata.readSlice(2)).as("content").isEqualByComparingTo(content);
    }

    @Test
    void encodeCompositeMetadata() {
        final Entry entry1 = new CustomTypeEntry("foo",
                ByteBufUtils.getRandomByteBuf(1));

        WellKnownMimeType mime2 = WellKnownMimeType.fromId(5);
        //5 = 0b00000101
        byte expected2 = (byte) 0b10000101;
        final Entry entry2 = new CompressedTypeEntry(mime2,
                ByteBufUtils.getRandomByteBuf(2));

        byte id3 = (byte) 120;
        byte expected3 = (byte) 0b11111000;
        final Entry entry3 = new UnknownCompressedTypeEntry(id3,
                ByteBufUtils.getRandomByteBuf(3));

        CompositeMetadata compositeMetadata = new CompositeMetadata(Arrays.asList(entry1, entry2, entry3));
        CompositeByteBuf buf = CompositeMetadata.encodeComposite(ByteBufAllocator.DEFAULT, compositeMetadata);

        assertThat(buf.readByte())
                .as("meta1 mime length")
                .isEqualTo((byte) 2);
        assertThat(buf.readCharSequence(3, CharsetUtil.US_ASCII).toString())
                .as("meta1 mime")
                .isEqualTo("foo");
        assertThat(buf.readUnsignedMedium())
                .as("meta1 content length")
                .isEqualTo(1);
        assertThat(buf.readBytes(1))
                .as("meta1 content")
                .isEqualByComparingTo(entry1.getMetadata());

        assertThat(buf.readByte())
                .as("meta2 id")
                .isEqualTo(expected2);
        assertThat(buf.readUnsignedMedium())
                .as("meta2 content length")
                .isEqualTo(2);
        assertThat(buf.readBytes(2))
                .as("meta2 content")
                .isEqualByComparingTo(entry2.getMetadata());

        assertThat(buf.readByte())
                .as("meta3 id")
                .isEqualTo(expected3);
        assertThat(buf.readUnsignedMedium())
                .as("meta3 content length")
                .isEqualTo(3);
        assertThat(buf.readBytes(3))
                .as("meta3 content")
                .isEqualByComparingTo(entry3.getMetadata());
    }

    @Test
    void getForTypeWithTwoMatches() {
        ByteBuf noMatch = ByteBufUtils.getRandomByteBuf(2);
        ByteBuf match1 = ByteBufUtils.getRandomByteBuf(2);
        ByteBuf match2 = ByteBufUtils.getRandomByteBuf(2);
        CompositeMetadata metadata = new CompositeMetadata(Arrays.asList(
                new CustomTypeEntry("noMatch", noMatch),
                new CustomTypeEntry("match", match1),
                new CustomTypeEntry("match", match2)
        ));

        assertThat(metadata.get("match"))
                .isNotNull()
                .extracting(Entry::getMetadata)
                .isSameAs(match1);
        assertThat(metadata.getAll("match"))
                .flatExtracting(Entry::getMetadata)
                .containsExactly(match1, match2);
    }

    @Test
    void getForTypeWithNoMatch() {
        ByteBuf noMatch1 = ByteBufUtils.getRandomByteBuf(2);
        ByteBuf noMatch2 = ByteBufUtils.getRandomByteBuf(2);
        CompositeMetadata metadata = new CompositeMetadata(Arrays.asList(
                new CustomTypeEntry("noMatch1", noMatch1),
                new CustomTypeEntry("noMatch2", noMatch2)
        ));

        assertThat(metadata.get("match")).isNull();
        assertThat(metadata.getAll("match"))
                .isEmpty();
    }

    @Test
    void getAllForTypeIsUnmodifiable() {
        ByteBuf match1 = ByteBufUtils.getRandomByteBuf(2);
        ByteBuf match2 = ByteBufUtils.getRandomByteBuf(2);
        CompositeMetadata metadata = new CompositeMetadata(Arrays.asList(
                new CustomTypeEntry("match1", match1),
                new CustomTypeEntry("match2", match2)
        ));

        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> metadata.getAll("match").clear());
    }

    @Test
    void getAllParseableVsGetAll() {
        final Entry entry1 = new CustomTypeEntry("foo", ByteBufUtils.getRandomByteBuf(2));
        final Entry entry2 = new CompressedTypeEntry(WellKnownMimeType.APPLICATION_GZIP, ByteBufUtils.getRandomByteBuf(2));
        final Entry entry3 = new UnknownCompressedTypeEntry((byte) 120, ByteBufUtils.getRandomByteBuf(2));

        CompositeMetadata metadata = new CompositeMetadata(
                Arrays.asList(entry1, entry2, entry3));

        assertThat(metadata.getAll())
                .as("getAll()")
                .containsExactly(entry1, entry2, entry3);
        assertThat(metadata.getAllParseable())
                .as("getAllParseable()")
                .containsExactly(entry1, entry2);
    }

    @Test
    void decodeEntryTooShortForMimeLength() {
        ByteBuf fakeEntry = ByteBufAllocator.DEFAULT.buffer();
        fakeEntry.writeByte(120);

        assertThatIllegalArgumentException()
                .isThrownBy(() -> CompositeMetadata.decodeEntry(fakeEntry, false))
                .withMessage("composite metadata entry buffer is too short to contain proper entry");
    }

    @Test
    void decodeEntryHasNoContentLength() {
        ByteBuf fakeEntry = ByteBufAllocator.DEFAULT.buffer();
        fakeEntry.writeByte(0);
        fakeEntry.writeCharSequence("w", CharsetUtil.US_ASCII);

        assertThatIllegalArgumentException()
                .isThrownBy(() -> CompositeMetadata.decodeEntry(fakeEntry, false))
                .withMessage("composite metadata entry buffer is too short to contain proper entry");
    }

    @Test
    void decodeEntryTooShortForContentLength() {
        ByteBuf fakeEntry = ByteBufAllocator.DEFAULT.buffer();
        fakeEntry.writeByte(1);
        fakeEntry.writeCharSequence("w", CharsetUtil.US_ASCII);
        NumberUtils.encodeUnsignedMedium(fakeEntry, 456);
        fakeEntry.writeChar('w');

        assertThatIllegalArgumentException()
                .isThrownBy(() -> CompositeMetadata.decodeEntry(fakeEntry, false))
                .withMessage("composite metadata entry buffer is too short to contain proper entry");
    }

    @Test
    void decodeEntryOnDoneBufferReturnsNull() {
        ByteBuf fakeBuffer = ByteBufUtils.getRandomByteBuf(0);

        assertThat(CompositeMetadata.decodeEntry(fakeBuffer, false))
                .as("empty entry")
                .isNull();
    }
}