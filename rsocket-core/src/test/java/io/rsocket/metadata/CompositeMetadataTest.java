package io.rsocket.metadata;

import static org.assertj.core.api.Assertions.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.rsocket.test.util.ByteBufUtils;
import io.rsocket.util.NumberUtils;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;

class CompositeMetadataTest {

  @Test
  void decodeEntryTooShortForMimeLength() {
    ByteBuf fakeEntry = Unpooled.buffer();
    fakeEntry.writeByte(120);
    CompositeMetadata compositeMetadata = new CompositeMetadata(fakeEntry, false);

    assertThatIllegalArgumentException()
        .isThrownBy(compositeMetadata::decodeNext)
        .withMessage("composite metadata entry buffer is too short to contain proper entry");
  }

  @Test
  void decodeEntryHasNoContentLength() {
    ByteBuf fakeEntry = Unpooled.buffer();
    fakeEntry.writeByte(0);
    fakeEntry.writeCharSequence("w", CharsetUtil.US_ASCII);
    CompositeMetadata compositeMetadata = new CompositeMetadata(fakeEntry, false);

    assertThatIllegalArgumentException()
        .isThrownBy(compositeMetadata::decodeNext)
        .withMessage("composite metadata entry buffer is too short to contain proper entry");
  }

  @Test
  void decodeEntryTooShortForContentLength() {
    ByteBuf fakeEntry = Unpooled.buffer();
    fakeEntry.writeByte(1);
    fakeEntry.writeCharSequence("w", CharsetUtil.US_ASCII);
    NumberUtils.encodeUnsignedMedium(fakeEntry, 456);
    fakeEntry.writeChar('w');
    CompositeMetadata compositeMetadata = new CompositeMetadata(fakeEntry, false);

    assertThatIllegalArgumentException()
        .isThrownBy(compositeMetadata::decodeNext)
        .withMessage("composite metadata entry buffer is too short to contain proper entry");
  }

  @Test
  void decodeEntryOnDoneBufferThrowsNoSuchElement() {
    ByteBuf fakeBuffer = ByteBufUtils.getRandomByteBuf(0);
    CompositeMetadata compositeMetadata = new CompositeMetadata(fakeBuffer, false);

    assertThatExceptionOfType(NoSuchElementException.class)
        .isThrownBy(compositeMetadata::decodeNext)
        .withMessage("composite metadata has no more entries");
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

    CompositeByteBuf compositeMetadataBuffer = ByteBufAllocator.DEFAULT.compositeBuffer();
    CompositeMetadataFlyweight.encodeAndAddMetadata(
        compositeMetadataBuffer, ByteBufAllocator.DEFAULT, mimeType1, metadata1);
    CompositeMetadataFlyweight.encodeAndAddMetadata(
        compositeMetadataBuffer, ByteBufAllocator.DEFAULT, mimeType2, metadata2);
    CompositeMetadataFlyweight.encodeAndAddMetadata(
        compositeMetadataBuffer, ByteBufAllocator.DEFAULT, reserved, metadata3);

    CompositeMetadata compositeMetadata = new CompositeMetadata(compositeMetadataBuffer, true);

    compositeMetadata.decodeNext();
    assertThat(compositeMetadata)
        .as("entry1")
        .isNotNull()
        .satisfies(
            e ->
                assertThat(e.getCurrentMimeType())
                    .as("entry1 mime type")
                    .isEqualTo(mimeType1.getMime()))
        .satisfies(
            e ->
                assertThat(e.getCurrentMimeId())
                    .as("entry1 mime id")
                    .isEqualTo((byte) mimeType1.getIdentifier()))
        .satisfies(
            e ->
                assertThat(e.getCurrentContent().toString(CharsetUtil.UTF_8))
                    .as("entry1 decoded")
                    .isEqualTo("abcdefghijkl"));

    compositeMetadata.decodeNext();
    assertThat(compositeMetadata)
        .as("entry2")
        .isNotNull()
        .satisfies(
            e -> assertThat(e.getCurrentMimeType()).as("entry2 mime type").isEqualTo(mimeType2))
        .satisfies(e -> assertThat(e.getCurrentMimeId()).as("entry2 mime id").isEqualTo((byte) -1))
        .satisfies(
            e ->
                assertThat(e.getCurrentContent())
                    .as("entry2 decoded")
                    .isEqualByComparingTo(metadata2));

    compositeMetadata.decodeNext();
    assertThat(compositeMetadata)
        .as("entry3")
        .isNotNull()
        .satisfies(e -> assertThat(e.getCurrentMimeType()).as("entry3 mime type").isNull())
        .satisfies(e -> assertThat(e.getCurrentMimeId()).as("entry3 mime id").isEqualTo(reserved))
        .satisfies(
            e ->
                assertThat(e.getCurrentContent())
                    .as("entry3 decoded")
                    .isEqualByComparingTo(metadata3));

    assertThat(compositeMetadata.hasNext()).as("has no more than 3 entries").isFalse();
  }
}
