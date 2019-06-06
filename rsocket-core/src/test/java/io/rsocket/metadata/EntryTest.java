package io.rsocket.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.rsocket.metadata.CompositeMetadataFlyweight.Entry;
import io.rsocket.test.util.ByteBufUtils;
import io.rsocket.util.NumberUtils;
import java.util.List;
import org.junit.jupiter.api.Test;

class EntryTest {

  @Test
  void encodeEntryWellKnownMetadata() {
    WellKnownMimeType type = WellKnownMimeType.fromId(5);
    // 5 = 0b00000101
    byte expected = (byte) 0b10000101;

    ByteBuf content = ByteBufUtils.getRandomByteBuf(2);
    Entry entry = new Entry(type.getMime(), type.getIdentifier(), content);

    final CompositeByteBuf metadata = ByteBufAllocator.DEFAULT.compositeBuffer();
    entry.encodeInto(metadata, ByteBufAllocator.DEFAULT);

    assertThat(metadata.readByte()).as("mime header").isEqualTo(expected);
    assertThat(metadata.readUnsignedMedium()).as("length header").isEqualTo(2);
    assertThat(metadata.readSlice(2)).as("content").isEqualByComparingTo(content);
  }

  @Test
  void encodeEntryCustomMetadata() {
    // length 3, encoded as length - 1 since 0 is not authorized
    byte expected = (byte) 2;
    ByteBuf content = ByteBufUtils.getRandomByteBuf(2);
    Entry entry = new Entry("foo", (byte) -1, content);

    final CompositeByteBuf metadata = ByteBufAllocator.DEFAULT.compositeBuffer();
    entry.encodeInto(metadata, ByteBufAllocator.DEFAULT);

    assertThat(metadata.readByte()).as("mime header").isEqualTo(expected);
    assertThat(metadata.readCharSequence(3, CharsetUtil.US_ASCII).toString()).isEqualTo("foo");
    assertThat(metadata.readUnsignedMedium()).as("length header").isEqualTo(2);
    assertThat(metadata.readSlice(2)).as("content").isEqualByComparingTo(content);
  }

  @Test
  void encodeEntryPassthroughMetadata() {
    // 120 = 0b01111000
    byte expected = (byte) 0b11111000;

    ByteBuf content = ByteBufUtils.getRandomByteBuf(2);
    Entry entry = new Entry(null, (byte) 120, content);

    final CompositeByteBuf metadata = ByteBufAllocator.DEFAULT.compositeBuffer();
    entry.encodeInto(metadata, ByteBufAllocator.DEFAULT);

    assertThat(metadata.readByte()).as("mime header").isEqualTo(expected);
    assertThat(metadata.readUnsignedMedium()).as("length header").isEqualTo(2);
    assertThat(metadata.readSlice(2)).as("content").isEqualByComparingTo(content);
  }

  @Test
  void decodeEntryTooShortForMimeLength() {
    ByteBuf fakeEntry = Unpooled.buffer();
    fakeEntry.writeByte(120);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> Entry.decode(fakeEntry, false))
        .withMessage("composite metadata entry buffer is too short to contain proper entry");
  }

  @Test
  void decodeEntryHasNoContentLength() {
    ByteBuf fakeEntry = Unpooled.buffer();
    fakeEntry.writeByte(0);
    fakeEntry.writeCharSequence("w", CharsetUtil.US_ASCII);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> Entry.decode(fakeEntry, false))
        .withMessage("composite metadata entry buffer is too short to contain proper entry");
  }

  @Test
  void decodeEntryTooShortForContentLength() {
    ByteBuf fakeEntry = Unpooled.buffer();
    fakeEntry.writeByte(1);
    fakeEntry.writeCharSequence("w", CharsetUtil.US_ASCII);
    NumberUtils.encodeUnsignedMedium(fakeEntry, 456);
    fakeEntry.writeChar('w');

    assertThatIllegalArgumentException()
        .isThrownBy(() -> Entry.decode(fakeEntry, false))
        .withMessage("composite metadata entry buffer is too short to contain proper entry");
  }

  @Test
  void decodeEntryOnDoneBufferReturnsNull() {
    ByteBuf fakeBuffer = ByteBufUtils.getRandomByteBuf(0);

    assertThat(Entry.decode(fakeBuffer, false)).as("empty entry").isNull();
  }

  @Test
  void decodeThreeEntries() {
    // metadata 1: well known
    WellKnownMimeType mimeType1 = WellKnownMimeType.APPLICATION_PDF;
    ByteBuf metadata1 = Unpooled.buffer();
    metadata1.writeCharSequence("abcdefghijkl", CharsetUtil.UTF_8);

    // metadata 2: custom
    String mimeType2 = "application/custom";
    ByteBuf metadata2 = Unpooled.buffer();
    metadata2.writeChar('E');
    metadata2.writeChar('∑');
    metadata2.writeChar('é');
    metadata2.writeBoolean(true);
    metadata2.writeChar('W');

    // metadata 3: reserved but unknown
    byte reserved = 120;
    assertThat(WellKnownMimeType.fromId(reserved))
        .as("ensure UNKNOWN RESERVED used in test")
        .isSameAs(WellKnownMimeType.UNKNOWN_RESERVED_MIME_TYPE);
    ByteBuf metadata3 = Unpooled.buffer();
    metadata3.writeByte(88);

    CompositeByteBuf compositeMetadata = ByteBufAllocator.DEFAULT.compositeBuffer();
    CompositeMetadataFlyweight.encodeAndAddMetadata(
        compositeMetadata, ByteBufAllocator.DEFAULT, mimeType1, metadata1);
    CompositeMetadataFlyweight.encodeAndAddMetadata(
        compositeMetadata, ByteBufAllocator.DEFAULT, mimeType2, metadata2);
    CompositeMetadataFlyweight.encodeAndAddMetadata(
        compositeMetadata, ByteBufAllocator.DEFAULT, reserved, metadata3);

    Entry entry1 = Entry.decode(compositeMetadata, true);
    Entry entry2 = Entry.decode(compositeMetadata, true);
    Entry entry3 = Entry.decode(compositeMetadata, true);
    Entry expectedNoMoreEntries = Entry.decode(compositeMetadata, true);

    assertThat(expectedNoMoreEntries).as("decodes exactly 3").isNull();
    assertThat(entry1)
        .as("entry1")
        .isNotNull()
        .satisfies(
            e -> assertThat(e.getMimeType()).as("entry1 mime type").isEqualTo(mimeType1.getMime()))
        .satisfies(
            e ->
                assertThat(e.getMimeId())
                    .as("entry1 mime id")
                    .isEqualTo((byte) mimeType1.getIdentifier()))
        .satisfies(
            e ->
                assertThat(e.getMetadata().toString(CharsetUtil.UTF_8))
                    .as("entry1 decoded")
                    .isEqualTo("abcdefghijkl"));

    assertThat(entry2)
        .as("entry2")
        .isNotNull()
        .satisfies(e -> assertThat(e.getMimeType()).as("entry2 mime type").isEqualTo(mimeType2))
        .satisfies(e -> assertThat(e.getMimeId()).as("entry2 mime id").isEqualTo((byte) -1))
        .satisfies(
            e -> assertThat(e.getMetadata()).as("entry2 decoded").isEqualByComparingTo(metadata2));

    assertThat(entry3)
        .as("entry3")
        .isNotNull()
        .satisfies(e -> assertThat(e.getMimeType()).as("entry3 mime type").isNull())
        .satisfies(e -> assertThat(e.getMimeId()).as("entry3 mime id").isEqualTo(reserved))
        .satisfies(
            e -> assertThat(e.getMetadata()).as("entry3 decoded").isEqualByComparingTo(metadata3));
  }

  @Test
  void decodeAllEntries() {
    // metadata 1: well known
    WellKnownMimeType mimeType1 = WellKnownMimeType.APPLICATION_PDF;
    ByteBuf metadata1 = Unpooled.buffer();
    metadata1.writeCharSequence("abcdefghijkl", CharsetUtil.UTF_8);

    // metadata 2: custom
    String mimeType2 = "application/custom";
    ByteBuf metadata2 = Unpooled.buffer();
    metadata2.writeChar('E');
    metadata2.writeChar('∑');
    metadata2.writeChar('é');
    metadata2.writeBoolean(true);
    metadata2.writeChar('W');

    // metadata 3: reserved but unknown
    byte reserved = 120;
    assertThat(WellKnownMimeType.fromId(reserved))
        .as("ensure UNKNOWN RESERVED used in test")
        .isSameAs(WellKnownMimeType.UNKNOWN_RESERVED_MIME_TYPE);
    ByteBuf metadata3 = Unpooled.buffer();
    metadata3.writeByte(88);

    CompositeByteBuf compositeMetadata = ByteBufAllocator.DEFAULT.compositeBuffer();
    CompositeMetadataFlyweight.encodeAndAddMetadata(
        compositeMetadata, ByteBufAllocator.DEFAULT, mimeType1, metadata1);
    CompositeMetadataFlyweight.encodeAndAddMetadata(
        compositeMetadata, ByteBufAllocator.DEFAULT, mimeType2, metadata2);
    CompositeMetadataFlyweight.encodeAndAddMetadata(
        compositeMetadata, ByteBufAllocator.DEFAULT, reserved, metadata3);

    List<Entry> decoded = Entry.decodeAll(compositeMetadata, true);

    assertThat(decoded).as("decodes exactly 3").hasSize(3);

    assertThat(decoded.get(0))
        .as("entry1")
        .isNotNull()
        .satisfies(
            e -> assertThat(e.getMimeType()).as("entry1 mime type").isEqualTo(mimeType1.getMime()))
        .satisfies(
            e ->
                assertThat(e.getMimeId())
                    .as("entry1 mime id")
                    .isEqualTo((byte) mimeType1.getIdentifier()))
        .satisfies(
            e ->
                assertThat(e.getMetadata().toString(CharsetUtil.UTF_8))
                    .as("entry1 decoded")
                    .isEqualTo("abcdefghijkl"));

    assertThat(decoded.get(1))
        .as("entry2")
        .isNotNull()
        .satisfies(e -> assertThat(e.getMimeType()).as("entry2 mime type").isEqualTo(mimeType2))
        .satisfies(e -> assertThat(e.getMimeId()).as("entry2 mime id").isEqualTo((byte) -1))
        .satisfies(
            e -> assertThat(e.getMetadata()).as("entry2 decoded").isEqualByComparingTo(metadata2));

    assertThat(decoded.get(2))
        .as("entry3")
        .isNotNull()
        .satisfies(e -> assertThat(e.getMimeType()).as("entry3 mime type").isNull())
        .satisfies(e -> assertThat(e.getMimeId()).as("entry3 mime id").isEqualTo(reserved))
        .satisfies(
            e -> assertThat(e.getMetadata()).as("entry3 decoded").isEqualByComparingTo(metadata3));
  }

  @Test
  void decodeAllForEmpty() {
    ByteBuf emptyBuffer = ByteBufAllocator.DEFAULT.buffer(0);
    assertThat(Entry.decodeAll(emptyBuffer, false)).isEmpty();
  }

  @Test
  void decodeAllForMalformed() {
    CompositeByteBuf compositeByteBuf = ByteBufAllocator.DEFAULT.compositeBuffer();
    // encode a first valid metadata
    WellKnownMimeType mimeType1 = WellKnownMimeType.APPLICATION_PDF;
    ByteBuf metadata1 = Unpooled.buffer();
    metadata1.writeCharSequence("abcdefghijkl", CharsetUtil.UTF_8);
    CompositeMetadataFlyweight.encodeAndAddMetadata(
        compositeByteBuf, ByteBufAllocator.DEFAULT, mimeType1, metadata1);
    // encode an invalid metadata
    compositeByteBuf.addComponents(true, ByteBufUtils.getRandomByteBuf(15));

    assertThatIllegalArgumentException()
        .isThrownBy(() -> Entry.decodeAll(compositeByteBuf, false))
        .withMessage("composite metadata entry buffer is too short to contain proper entry");
  }

  @Test
  void createCustomTypeEntry() {
    Entry entry = Entry.customMime("example/mime", ByteBufUtils.getRandomByteBuf(5));

    assertThat(entry.getMimeType()).as("mime type").isEqualTo("example/mime");
    assertThat(entry.getMimeId()).as("mime id").isEqualTo((byte) -1);
    assertThat(entry.getMetadata().isReadable(5)).as("5 bytes content").isTrue();
  }

  @Test
  void createWellKnownTypeEntry() {
    WellKnownMimeType wkn = WellKnownMimeType.APPLICATION_XML;
    Entry entry = Entry.wellKnownMime(wkn, ByteBufUtils.getRandomByteBuf(5));

    assertThat(entry.getMimeType()).as("mime type").isEqualTo(wkn.getMime());
    assertThat(entry.getMimeId()).as("mime id").isEqualTo(wkn.getIdentifier());
    assertThat(entry.getMetadata().isReadable(5)).as("5 bytes content").isTrue();
  }

  @Test
  void createCompressedRawTypeEntry() {
    byte id = (byte) 120;
    Entry entry = Entry.rawCompressedMime(id, ByteBufUtils.getRandomByteBuf(5));

    assertThat(entry.getMimeType()).as("mime type").isNull();
    assertThat(entry.getMimeId()).as("mime id").isEqualTo(id);
    assertThat(entry.getMetadata().isReadable(5)).as("5 bytes content").isTrue();
  }
}
