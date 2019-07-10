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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.rsocket.metadata.CompositeMetadata.Entry;
import io.rsocket.metadata.CompositeMetadata.ReservedMimeTypeEntry;
import io.rsocket.metadata.CompositeMetadata.WellKnownMimeTypeEntry;
import io.rsocket.test.util.ByteBufUtils;
import io.rsocket.util.NumberUtils;
import java.util.Iterator;
import java.util.Spliterator;
import org.junit.jupiter.api.Test;

class CompositeMetadataTest {

  @Test
  void decodeEntryHasNoContentLength() {
    ByteBuf fakeEntry = Unpooled.buffer();
    fakeEntry.writeByte(0);
    fakeEntry.writeCharSequence("w", CharsetUtil.US_ASCII);
    CompositeMetadata compositeMetadata = new CompositeMetadata(fakeEntry, false);

    assertThatIllegalStateException()
        .isThrownBy(() -> compositeMetadata.iterator().next())
        .withMessage("metadata is malformed");
  }

  @Test
  void decodeEntryOnDoneBufferThrowsIllegalArgument() {
    ByteBuf fakeBuffer = ByteBufUtils.getRandomByteBuf(0);
    CompositeMetadata compositeMetadata = new CompositeMetadata(fakeBuffer, false);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> compositeMetadata.iterator().next())
        .withMessage("entry index 0 is larger than buffer size");
  }

  @Test
  void decodeEntryTooShortForContentLength() {
    ByteBuf fakeEntry = Unpooled.buffer();
    fakeEntry.writeByte(1);
    fakeEntry.writeCharSequence("w", CharsetUtil.US_ASCII);
    NumberUtils.encodeUnsignedMedium(fakeEntry, 456);
    fakeEntry.writeChar('w');
    CompositeMetadata compositeMetadata = new CompositeMetadata(fakeEntry, false);

    assertThatIllegalStateException()
        .isThrownBy(() -> compositeMetadata.iterator().next())
        .withMessage("metadata is malformed");
  }

  @Test
  void decodeEntryTooShortForMimeLength() {
    ByteBuf fakeEntry = Unpooled.buffer();
    fakeEntry.writeByte(120);
    CompositeMetadata compositeMetadata = new CompositeMetadata(fakeEntry, false);

    assertThatIllegalStateException()
        .isThrownBy(() -> compositeMetadata.iterator().next())
        .withMessage("metadata is malformed");
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
    assertThat(WellKnownMimeType.fromIdentifier(reserved))
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

    Iterator<Entry> iterator = new CompositeMetadata(compositeMetadataBuffer, true).iterator();

    assertThat(iterator.next())
        .as("entry1")
        .isNotNull()
        .satisfies(
            e ->
                assertThat(e.getMimeType()).as("entry1 mime type").isEqualTo(mimeType1.getString()))
        .satisfies(
            e ->
                assertThat(((WellKnownMimeTypeEntry) e).getType())
                    .as("entry1 mime id")
                    .isEqualTo(WellKnownMimeType.APPLICATION_PDF))
        .satisfies(
            e ->
                assertThat(e.getContent().toString(CharsetUtil.UTF_8))
                    .as("entry1 decoded")
                    .isEqualTo("abcdefghijkl"));

    assertThat(iterator.next())
        .as("entry2")
        .isNotNull()
        .satisfies(e -> assertThat(e.getMimeType()).as("entry2 mime type").isEqualTo(mimeType2))
        .satisfies(
            e -> assertThat(e.getContent()).as("entry2 decoded").isEqualByComparingTo(metadata2));

    assertThat(iterator.next())
        .as("entry3")
        .isNotNull()
        .satisfies(e -> assertThat(e.getMimeType()).as("entry3 mime type").isNull())
        .satisfies(
            e ->
                assertThat(((ReservedMimeTypeEntry) e).getType())
                    .as("entry3 mime id")
                    .isEqualTo(reserved))
        .satisfies(
            e -> assertThat(e.getContent()).as("entry3 decoded").isEqualByComparingTo(metadata3));

    assertThat(iterator.hasNext()).as("has no more than 3 entries").isFalse();
  }

  @Test
  void streamIsNotParallel() {
    final CompositeMetadata metadata =
        new CompositeMetadata(ByteBufUtils.getRandomByteBuf(5), false);

    assertThat(metadata.stream().isParallel()).as("isParallel").isFalse();
  }

  @Test
  void streamSpliteratorCharacteristics() {
    final CompositeMetadata metadata =
        new CompositeMetadata(ByteBufUtils.getRandomByteBuf(5), false);

    assertThat(metadata.stream().spliterator())
        .matches(s -> s.hasCharacteristics(Spliterator.ORDERED), "ORDERED")
        .matches(s -> s.hasCharacteristics(Spliterator.DISTINCT), "DISTINCT")
        .matches(s -> s.hasCharacteristics(Spliterator.NONNULL), "NONNULL")
        .matches(s -> !s.hasCharacteristics(Spliterator.SIZED), "not SIZED");
  }
}
