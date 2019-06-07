package io.rsocket.metadata;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import java.util.NoSuchElementException;
import reactor.util.annotation.Nullable;

/**
 * An iterator-like wrapper around a {@link ByteBuf} that exposes metadata entry information at each
 * decoding step. This is only possible on frame types used to initiate interactions, if the SETUP
 * metadata mime type was {@link WellKnownMimeType#MESSAGE_RSOCKET_COMPOSITE_METADATA}.
 *
 * <p>This allows efficient incremental decoding of the entries (which moves the source's {@link
 * io.netty.buffer.ByteBuf#readerIndex()}). The buffer is assumed to contain just enough bytes to
 * represent one or more entries (mime type compressed or not). The decoding stops when the buffer
 * reaches 0 readable bytes ({@code hasNext() == false}), and fails if it contains bytes but not
 * enough to correctly decode an entry.
 *
 * <p>A note on future-proofness: it is possible to come across a compressed mime type that this
 * implementation doesn't recognize. This is likely to be due to the use of a byte id that is merely
 * reserved in this implementation, but maps to a {@link WellKnownMimeType} in the implementation
 * that encoded the metadata. This can be detected by {@link #getCurrentMimeId()} returning a
 * positive {@code byte} while {@link #getCurrentMimeType()} returns {@literal null}. The byte and
 * content buffer should be kept around and re-encoded using {@link
 * CompositeMetadataFlyweight#encodeAndAddMetadata(CompositeByteBuf, ByteBufAllocator, byte,
 * ByteBuf)} in case passing that entry through is required.
 */
public final class CompositeMetadata {

  private final ByteBuf source;
  private final boolean retainSlices;

  private byte id;
  private @Nullable String mime;
  private ByteBuf content;
  private int nextEntryIndex;

  /**
   * Wrap a composite metadata {@link ByteBuf} to allow incremental decoding of its entries. Each
   * decoded {@link ByteBuf} is either a {@link ByteBuf#slice()} or a {@link
   * ByteBuf#retainedSlice()} of the original buffer, depending on the {@code retainSlices}
   * parameter.
   *
   * @param fullCompositeMetadataBuffer
   */
  public CompositeMetadata(ByteBuf fullCompositeMetadataBuffer, boolean retainSlices) {
    this.source = fullCompositeMetadataBuffer;
    this.retainSlices = retainSlices;
    this.id = -1;
    this.mime = null;
    this.content = null;
    this.nextEntryIndex = 0;
  }

  /**
   * Wrap a composite metadata {@link ByteBuf} to allow incremental decoding of its entries. Each
   * decoded {@link ByteBuf} is a {@link ByteBuf#retainedSlice()} of the original buffer.
   *
   * @param fullCompositeMetadataBuffer
   */
  public CompositeMetadata(ByteBuf fullCompositeMetadataBuffer) {
    this(fullCompositeMetadataBuffer, true);
  }

  /**
   * Return true if the source buffer still has readable bytes, which is assumed to mean at least
   * one more decodable entry.
   *
   * @return true if the source buffer still has readable bytes
   */
  public boolean hasNext() {
    return source.writerIndex() - nextEntryIndex > 0;
  }

  private void reset() {
    this.id = -1;
    this.mime = null;
    this.content = null;
  }

  /**
   * Decode the next entry in the source buffer, making its values accessible through the {@link
   * #getCurrentMimeType()}, {@link #getCurrentMimeId()} and {@link #getCurrentContent()} accessors.
   *
   * @throws IllegalArgumentException if the buffer contains more data but that data cannot be
   *     decoded as an entry
   * @throws NoSuchElementException if the buffer contains no more data (which can be avoided by
   *     checking {@link #hasNext()})
   */
  public void decodeNext() {
    reset();
    ByteBuf[] decoded =
        CompositeMetadataFlyweight.decodeMimeAndContentBuffersSlices(
            source, nextEntryIndex, retainSlices);
    if (decoded == CompositeMetadataFlyweight.METADATA_MALFORMED) {
      throw new IllegalArgumentException(
          "composite metadata entry buffer is too short to contain proper entry");
    }
    if (decoded == CompositeMetadataFlyweight.METADATA_BUFFERS_DONE) {
      throw new NoSuchElementException("composite metadata has no more entries");
    }

    ByteBuf header = decoded[0];
    this.content = decoded[1];
    // move the nextEntryIndex
    this.nextEntryIndex =
        CompositeMetadataFlyweight.computeNextEntryIndex(this.nextEntryIndex, header, content);

    if (header.readableBytes() == 1) {
      this.id = CompositeMetadataFlyweight.decodeMimeIdFromMimeBuffer(header);
      WellKnownMimeType wkn = WellKnownMimeType.fromId(id);
      if (wkn != WellKnownMimeType.UNKNOWN_RESERVED_MIME_TYPE) {
        this.mime = wkn.getMime();
      }
    } else {
      this.id = -1;
      CharSequence charSequence = CompositeMetadataFlyweight.decodeMimeTypeFromMimeBuffer(header);
      if (charSequence == null) {
        throw new IllegalArgumentException(
            "composite metadata entry parsing failed on custom type");
      }
      this.mime = charSequence.toString();
    }
  }

  /**
   * Returns the mime type {@link String} representation of the currently decoded entry, if there is
   * one.
   *
   * <p>A {@code null} value should only occur with a positive {@link #getCurrentMimeId()}, denoting
   * an entry that is compressed but unparseable (probably a buffer encoded by another version of
   * the well known mime type extension spec, which is only reserved in this implementation).
   *
   * @return the mime type for this entry, or null
   */
  @Nullable
  public String getCurrentMimeType() {
    return this.mime;
  }

  /**
   * @return the compressed mime id byte if relevant (0-127), or -1 if not (custom mime type as
   *     {@link String})
   */
  public byte getCurrentMimeId() {
    return this.id;
  }

  /** @return the metadata content of the currently decoded entry */
  public ByteBuf getCurrentContent() {
    return this.content;
  }
}
