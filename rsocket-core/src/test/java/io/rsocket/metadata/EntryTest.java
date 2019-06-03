package io.rsocket.metadata;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.CharsetUtil;
import io.rsocket.metadata.CompositeMetadataFlyweight.Entry;
import io.rsocket.metadata.CompositeMetadataFlyweight.Entry.CompressedTypeEntry;
import io.rsocket.metadata.CompositeMetadataFlyweight.Entry.CustomTypeEntry;
import io.rsocket.test.util.ByteBufUtils;
import io.rsocket.util.NumberUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class EntryTest {

    @Test
    void encodeEntryWellKnownMetadata() {
        WellKnownMimeType type = WellKnownMimeType.fromId(5);
        //5 = 0b00000101
        byte expected = (byte) 0b10000101;

        ByteBuf content = ByteBufUtils.getRandomByteBuf(2);
        Entry entry = new CompressedTypeEntry(type, content);

        final CompositeByteBuf metadata = ByteBufAllocator.DEFAULT.compositeBuffer();
        entry.encodeInto(metadata, ByteBufAllocator.DEFAULT);

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
        entry.encodeInto(metadata, ByteBufAllocator.DEFAULT);

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
        Entry.UnknownCompressedTypeEntry entry = new Entry.UnknownCompressedTypeEntry((byte) 120, content);

        final CompositeByteBuf metadata = ByteBufAllocator.DEFAULT.compositeBuffer();
        entry.encodeInto(metadata, ByteBufAllocator.DEFAULT);

        assertThat(metadata.readByte())
                .as("mime header")
                .isEqualTo(expected);
        assertThat(metadata.readUnsignedMedium()).as("length header").isEqualTo(2);
        assertThat(metadata.readSlice(2)).as("content").isEqualByComparingTo(content);
    }

    @Test
    void decodeEntryTooShortForMimeLength() {
        ByteBuf fakeEntry = ByteBufAllocator.DEFAULT.buffer();
        fakeEntry.writeByte(120);

        assertThatIllegalArgumentException()
                .isThrownBy(() -> Entry.decodeEntry(fakeEntry, false))
                .withMessage("composite metadata entry buffer is too short to contain proper entry");
    }

    @Test
    void decodeEntryHasNoContentLength() {
        ByteBuf fakeEntry = ByteBufAllocator.DEFAULT.buffer();
        fakeEntry.writeByte(0);
        fakeEntry.writeCharSequence("w", CharsetUtil.US_ASCII);

        assertThatIllegalArgumentException()
                .isThrownBy(() -> Entry.decodeEntry(fakeEntry, false))
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
                .isThrownBy(() -> Entry.decodeEntry(fakeEntry, false))
                .withMessage("composite metadata entry buffer is too short to contain proper entry");
    }

    @Test
    void decodeEntryOnDoneBufferReturnsNull() {
        ByteBuf fakeBuffer = ByteBufUtils.getRandomByteBuf(0);

        assertThat(Entry.decodeEntry(fakeBuffer, false))
                .as("empty entry")
                .isNull();
    }
}