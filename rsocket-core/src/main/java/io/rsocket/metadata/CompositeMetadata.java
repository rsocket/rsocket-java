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

import static io.rsocket.metadata.CompositeMetadataFlyweight.computeNextEntryIndex;
import static io.rsocket.metadata.CompositeMetadataFlyweight.decodeMimeAndContentBuffersSlices;
import static io.rsocket.metadata.CompositeMetadataFlyweight.decodeMimeIdFromMimeBuffer;
import static io.rsocket.metadata.CompositeMetadataFlyweight.decodeMimeTypeFromMimeBuffer;
import static io.rsocket.metadata.CompositeMetadataFlyweight.hasEntry;
import static io.rsocket.metadata.CompositeMetadataFlyweight.isWellKnownMimeType;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.rsocket.metadata.CompositeMetadata.Entry;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import reactor.util.annotation.Nullable;

/**
 * An {@link Iterable} wrapper around a {@link ByteBuf} that exposes metadata entry information at
 * each decoding step. This is only possible on frame types used to initiate interactions, if the
 * SETUP metadata mime type was {@link WellKnownMimeType#MESSAGE_RSOCKET_COMPOSITE_METADATA}.
 *
 * <p>This allows efficient incremental decoding of the entries (without moving the source's {@link
 * io.netty.buffer.ByteBuf#readerIndex()}). The buffer is assumed to contain just enough bytes to
 * represent one or more entries (mime type compressed or not). The decoding stops when the buffer
 * reaches 0 readable bytes, and fails if it contains bytes but not enough to correctly decode an
 * entry.
 *
 * <p>A note on future-proofness: it is possible to come across a compressed mime type that this
 * implementation doesn't recognize. This is likely to be due to the use of a byte id that is merely
 * reserved in this implementation, but maps to a {@link WellKnownMimeType} in the implementation
 * that encoded the metadata. This can be detected by detecting that an entry is a {@link
 * ReservedMimeTypeEntry}. In this case {@link Entry#getMimeType()} will return {@code null}. The
 * encoded id can be retrieved using {@link ReservedMimeTypeEntry#getType()}. The byte and content
 * buffer should be kept around and re-encoded using {@link
 * CompositeMetadataFlyweight#encodeAndAddMetadata(CompositeByteBuf, ByteBufAllocator, byte,
 * ByteBuf)} in case passing that entry through is required.
 */
public final class CompositeMetadata implements Iterable<Entry> {

  private final boolean retainSlices;

  private final ByteBuf source;

  public CompositeMetadata(ByteBuf source, boolean retainSlices) {
    this.source = source;
    this.retainSlices = retainSlices;
  }

  /**
   * Turn this {@link CompositeMetadata} into a sequential {@link Stream}.
   *
   * @return the composite metadata sequential {@link Stream}
   */
  public Stream<Entry> stream() {
    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(
            iterator(), Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.ORDERED),
        false);
  }

  /**
   * An {@link Iterator} that lazily decodes {@link Entry} in this composite metadata.
   *
   * @return the composite metadata {@link Iterator}
   */
  @Override
  public Iterator<Entry> iterator() {
    return new Iterator<Entry>() {

      private int entryIndex = 0;

      @Override
      public boolean hasNext() {
        return hasEntry(CompositeMetadata.this.source, this.entryIndex);
      }

      @Override
      public Entry next() {
        ByteBuf[] headerAndData =
            decodeMimeAndContentBuffersSlices(
                CompositeMetadata.this.source,
                this.entryIndex,
                CompositeMetadata.this.retainSlices);

        ByteBuf header = headerAndData[0];
        ByteBuf data = headerAndData[1];

        this.entryIndex = computeNextEntryIndex(this.entryIndex, header, data);

        if (!isWellKnownMimeType(header)) {
          CharSequence typeString = decodeMimeTypeFromMimeBuffer(header);
          if (typeString == null) {
            throw new IllegalStateException("MIME type cannot be null");
          }

          return new ExplicitMimeTimeEntry(data, typeString.toString());
        }

        byte id = decodeMimeIdFromMimeBuffer(header);
        WellKnownMimeType type = WellKnownMimeType.fromIdentifier(id);

        if (WellKnownMimeType.UNKNOWN_RESERVED_MIME_TYPE == type) {
          return new ReservedMimeTypeEntry(data, id);
        }

        return new WellKnownMimeTypeEntry(data, type);
      }
    };
  }

  /** An entry in the {@link CompositeMetadata}. */
  public interface Entry {

    /**
     * Returns the un-decoded content of the {@link Entry}.
     *
     * @return the un-decoded content of the {@link Entry}
     */
    ByteBuf getContent();

    /**
     * Returns the MIME type of the entry, if it can be decoded.
     *
     * @return the MIME type of the entry, if it can be decoded, otherwise {@code null}.
     */
    @Nullable
    String getMimeType();
  }

  /** An {@link Entry} backed by an explicitly declared MIME type. */
  public static final class ExplicitMimeTimeEntry implements Entry {

    private final ByteBuf content;

    private final String type;

    public ExplicitMimeTimeEntry(ByteBuf content, String type) {
      this.content = content;
      this.type = type;
    }

    @Override
    public ByteBuf getContent() {
      return this.content;
    }

    @Override
    public String getMimeType() {
      return this.type;
    }
  }

  /**
   * An {@link Entry} backed by a {@link WellKnownMimeType} entry, but one that is not understood by
   * this implementation.
   */
  public static final class ReservedMimeTypeEntry implements Entry {
    private final ByteBuf content;
    private final int type;

    public ReservedMimeTypeEntry(ByteBuf content, int type) {
      this.content = content;
      this.type = type;
    }

    @Override
    public ByteBuf getContent() {
      return this.content;
    }

    /**
     * {@inheritDoc} Since this entry represents a compressed id that couldn't be decoded, this is
     * always {@code null}.
     */
    @Override
    public String getMimeType() {
      return null;
    }

    /**
     * Returns the reserved, but unknown {@link WellKnownMimeType} for this entry. Range is 0-127
     * (inclusive).
     *
     * @return the reserved, but unknown {@link WellKnownMimeType} for this entry
     */
    public int getType() {
      return this.type;
    }
  }

  /** An {@link Entry} backed by a {@link WellKnownMimeType}. */
  public static final class WellKnownMimeTypeEntry implements Entry {

    private final ByteBuf content;
    private final WellKnownMimeType type;

    public WellKnownMimeTypeEntry(ByteBuf content, WellKnownMimeType type) {
      this.content = content;
      this.type = type;
    }

    @Override
    public ByteBuf getContent() {
      return this.content;
    }

    @Override
    public String getMimeType() {
      return this.type.getString();
    }

    /**
     * Returns the {@link WellKnownMimeType} for this entry.
     *
     * @return the {@link WellKnownMimeType} for this entry
     */
    public WellKnownMimeType getType() {
      return this.type;
    }
  }
}
