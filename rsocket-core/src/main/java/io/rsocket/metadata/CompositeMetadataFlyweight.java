package io.rsocket.metadata;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.CharsetUtil;
import io.rsocket.util.NumberUtils;
import java.util.ArrayList;
import java.util.List;
import reactor.util.annotation.Nullable;

/**
 * A flyweight class that can be used to encode/decode composite metadata information to/from {@link
 * ByteBuf}. This is intended for low-level efficient manipulation of such buffers, but each
 * composite metadata entry can be also manipulated as an higher abstraction {@link Entry} class,
 * which provides its own encoding and decoding primitives.
 */
public class CompositeMetadataFlyweight {

  static final int STREAM_METADATA_KNOWN_MASK = 0x80; // 1000 0000
  static final byte STREAM_METADATA_LENGTH_MASK = 0x7F; // 0111 1111

  /**
   * Denotes that an attempt at 0-garbage decoding failed because the input buffer didn't have
   * enough bytes to represent a complete metadata entry, only part of the bytes.
   */
  public static final ByteBuf[] METADATA_MALFORMED = new ByteBuf[0];
  /**
   * Denotes that an attempt at garbage-free decoding failed because the input buffer was completely
   * empty, which generally means that no more entries are present in the buffer.
   */
  public static final ByteBuf[] METADATA_BUFFERS_DONE = new ByteBuf[0];
  /**
   * Denotes that an attempt at higher level decoding of an entry components failed because the
   * input buffer was completely empty, which generally means that no more entries are present in
   * the buffer.
   */
  static final Object[] METADATA_ENTRIES_DONE = new Object[0];

  private CompositeMetadataFlyweight() {}

  /**
   * Decode the next metadata entry (a mime header + content pair of {@link ByteBuf}) from a {@link
   * ByteBuf} that contains at least enough bytes for one more such entry. The header buffer is
   * either:
   *
   * <ul>
   *   <li>made up of a single byte: this represents an encoded mime id, which can be further
   *       decoded using {@link #decodeMimeIdFromMimeBuffer(ByteBuf)}
   *   <li>made up of 2 or more bytes: this represents an encoded mime String + its length, which
   *       can be further decoded using {@link #decodeMimeTypeFromMimeBuffer(ByteBuf)}. Note the
   *       encoded length, in the first byte, is skipped by this decoding method because the
   *       remaining length of the buffer is that of the mime string.
   * </ul>
   *
   * Moreover, if the source buffer is empty of readable bytes it is assumed that the composite has
   * been decoded entirely and the {@link #METADATA_BUFFERS_DONE} constant is returned. If the
   * buffer contains <i>some</i> readable bytes but not enough for a correct representation of an
   * entry, the {@link #METADATA_MALFORMED} constant is returned.
   *
   * @param compositeMetadata the source {@link ByteBuf} that originally contains one or more
   *     metadata entries
   * @param retainSlices should produced metadata entry buffers {@link ByteBuf#slice() slices} be
   *     {@link ByteBuf#retainedSlice() retained}?
   * @return a {@link ByteBuf} slice array of length 2 containing the mime header buffer and the
   *     content buffer, or one of the zero-length error constant arrays
   */
  public static ByteBuf[] decodeMimeAndContentBuffers(
      ByteBuf compositeMetadata, boolean retainSlices) {
    if (compositeMetadata.isReadable()) {
      ByteBuf mime;
      int ridx = compositeMetadata.readerIndex();
      byte mimeIdOrLength = compositeMetadata.readByte();
      if ((mimeIdOrLength & STREAM_METADATA_KNOWN_MASK) == STREAM_METADATA_KNOWN_MASK) {
        mime =
            retainSlices
                ? compositeMetadata.retainedSlice(ridx, 1)
                : compositeMetadata.slice(ridx, 1);
      } else {
        // M flag unset, remaining 7 bits are the length of the mime
        int mimeLength = Byte.toUnsignedInt(mimeIdOrLength) + 1;

        if (compositeMetadata.isReadable(
            mimeLength)) { // need to be able to read an extra mimeLength bytes
          // here we need a way for the returned ByteBuf to differentiate between a
          // 1-byte length mime type and a 1 byte encoded mime id, preferably without
          // re-applying the byte mask. The easiest way is to include the initial byte
          // and have further decoding ignore the first byte. 1 byte buffer == id, 2+ byte
          // buffer == full mime string.
          mime =
              retainSlices
                  ?
                  // we accommodate that we don't read from current readerIndex, but
                  // readerIndex - 1 ("0"), for a total slice size of mimeLength + 1
                  compositeMetadata.retainedSlice(ridx, mimeLength + 1)
                  : compositeMetadata.slice(ridx, mimeLength + 1);
          // we thus need to skip the bytes we just sliced, but not the flag/length byte
          // which was already skipped in initial read
          compositeMetadata.skipBytes(mimeLength);
        } else {
          return METADATA_MALFORMED;
        }
      }

      if (compositeMetadata.isReadable(3)) {
        // ensures the length medium can be read
        final int metadataLength = compositeMetadata.readUnsignedMedium();
        if (compositeMetadata.isReadable(metadataLength)) {
          ByteBuf metadata =
              retainSlices
                  ? compositeMetadata.readRetainedSlice(metadataLength)
                  : compositeMetadata.readSlice(metadataLength);
          return new ByteBuf[] {mime, metadata};
        } else {
          return METADATA_MALFORMED;
        }
      } else {
        return METADATA_MALFORMED;
      }
    }
    return METADATA_BUFFERS_DONE;
  }

  /**
   * Decode a {@code byte} compressed mime id from a {@link ByteBuf}, assuming said buffer properly
   * contains such an id.
   *
   * <p>The buffer must have exactly one readable byte, which is assumed to have been tested for
   * mime id encoding via the {@link #STREAM_METADATA_KNOWN_MASK} mask ({@code firstByte &
   * STREAM_METADATA_KNOWN_MASK) == STREAM_METADATA_KNOWN_MASK}).
   *
   * <p>If there is no readable byte, the negative identifier of {@link
   * WellKnownMimeType#UNPARSEABLE_MIME_TYPE} is returned.
   *
   * @param mimeBuffer the buffer that should next contain the compressed mime id byte
   * @return the compressed mime id, between 0 and 127, or a negative id if the input is invalid
   * @see #decodeMimeTypeFromMimeBuffer(ByteBuf)
   */
  public static byte decodeMimeIdFromMimeBuffer(ByteBuf mimeBuffer) {
    if (mimeBuffer.readableBytes() != 1) {
      return WellKnownMimeType.UNPARSEABLE_MIME_TYPE.getIdentifier();
    }
    return (byte) (mimeBuffer.readByte() & STREAM_METADATA_LENGTH_MASK);
  }

  /**
   * Decode a {@link CharSequence} custome mime type from a {@link ByteBuf}, assuming said buffer
   * properly contains such a mime type.
   *
   * <p>The buffer must at least have two readable bytes, which distinguishes it from the {@link
   * #decodeMimeIdFromMimeBuffer(ByteBuf) compressed id} case. The first byte is a size and the
   * remaining bytes must correspond to the {@link CharSequence}, encoded fully in US_ASCII. As a
   * result, the first byte can simply be skipped, and the remaining of the buffer be decoded to the
   * mime type.
   *
   * <p>If the mime header buffer is less than 2 bytes long, returns {@code null}.
   *
   * @param flyweightMimeBuffer the mime header {@link ByteBuf} that contains length + custom mime
   *     type
   * @return the decoded custom mime type, as a {@link CharSequence}, or null if the input is
   *     invalid
   * @see #decodeMimeIdFromMimeBuffer(ByteBuf)
   */
  @Nullable
  public static CharSequence decodeMimeTypeFromMimeBuffer(ByteBuf flyweightMimeBuffer) {
    if (flyweightMimeBuffer.readableBytes() < 2) {
      return null;
    }
    // the encoded length is assumed to be kept at the start of the buffer
    // but also assumed to be irrelevant because the rest of the slice length
    // actually already matches _decoded_length
    flyweightMimeBuffer.skipBytes(1);
    int mimeStringLength = flyweightMimeBuffer.readableBytes();
    return flyweightMimeBuffer.readCharSequence(mimeStringLength, CharsetUtil.US_ASCII);
  }

  /**
   * Encode a {@link WellKnownMimeType well known mime type} and a metadata value length into a
   * newly allocated {@link ByteBuf}.
   *
   * <p>This compact representation encodes the mime type via its ID on a single byte, and the
   * unsigned value length on 3 additional bytes.
   *
   * @param allocator the {@link ByteBufAllocator} to use to create the buffer.
   * @param mimeType a byte identifier of a {@link WellKnownMimeType} to encode.
   * @param metadataLength the metadata length to append to the buffer as an unsigned 24 bits
   *     integer.
   * @return the encoded mime and metadata length information
   */
  static ByteBuf encodeMetadataHeader(
      ByteBufAllocator allocator, byte mimeType, int metadataLength) {
    ByteBuf buffer = allocator.buffer(4, 4).writeByte(mimeType | STREAM_METADATA_KNOWN_MASK);

    NumberUtils.encodeUnsignedMedium(buffer, metadataLength);

    return buffer;
  }

  /**
   * Encode a custom mime type and a metadata value length into a newly allocated {@link ByteBuf}.
   *
   * <p>This larger representation encodes the mime type representation's length on a single byte,
   * then the representation itself, then the unsigned metadata value length on 3 additional bytes.
   *
   * @param allocator the {@link ByteBufAllocator} to use to create the buffer.
   * @param customMime a custom mime type to encode.
   * @param metadataLength the metadata length to append to the buffer as an unsigned 24 bits
   *     integer.
   * @return the encoded mime and metadata length information
   */
  static ByteBuf encodeMetadataHeader(
      ByteBufAllocator allocator, String customMime, int metadataLength) {
    ByteBuf metadataHeader = allocator.buffer(4 + customMime.length());
    // reserve 1 byte for the customMime length
    int writerIndexInitial = metadataHeader.writerIndex();
    metadataHeader.writerIndex(writerIndexInitial + 1);

    // write the custom mime in UTF8 but validate it is all ASCII-compatible
    // (which produces the right result since ASCII chars are still encoded on 1 byte in UTF8)
    int customMimeLength = ByteBufUtil.writeUtf8(metadataHeader, customMime);
    if (!ByteBufUtil.isText(metadataHeader, CharsetUtil.US_ASCII)) {
      metadataHeader.release();
      throw new IllegalArgumentException("custom mime type must be US_ASCII characters only");
    }
    if (customMimeLength < 1 || customMimeLength > 128) {
      metadataHeader.release();
      throw new IllegalArgumentException(
          "custom mime type must have a strictly positive length that fits on 7 unsigned bits, ie 1-128");
    }
    metadataHeader.markWriterIndex();

    // go back to beginning and write the length
    // encoded length is one less than actual length, since 0 is never a valid length, which gives
    // wider representation range
    metadataHeader.writerIndex(writerIndexInitial);
    metadataHeader.writeByte(customMimeLength - 1);

    // go back to post-mime type and write the metadata content length
    metadataHeader.resetWriterIndex();
    NumberUtils.encodeUnsignedMedium(metadataHeader, metadataLength);

    return metadataHeader;
  }

  /**
   * Encode a new sub-metadata information into a composite metadata {@link CompositeByteBuf
   * buffer}.
   *
   * @param compositeMetaData the buffer that will hold all composite metadata information.
   * @param allocator the {@link ByteBufAllocator} to use to create intermediate buffers as needed.
   * @param customMimeType the custom mime type to encode.
   * @param metadata the metadata value to encode.
   */
  // see #encodeMetadataHeader(ByteBufAllocator, String, int)
  public static void encodeAndAddMetadata(
      CompositeByteBuf compositeMetaData,
      ByteBufAllocator allocator,
      String customMimeType,
      ByteBuf metadata) {
    compositeMetaData.addComponents(
        true, encodeMetadataHeader(allocator, customMimeType, metadata.readableBytes()), metadata);
  }

  /**
   * Encode a new sub-metadata information into a composite metadata {@link CompositeByteBuf
   * buffer}.
   *
   * @param compositeMetaData the buffer that will hold all composite metadata information.
   * @param allocator the {@link ByteBufAllocator} to use to create intermediate buffers as needed.
   * @param knownMimeType the {@link WellKnownMimeType} to encode.
   * @param metadata the metadata value to encode.
   */
  // see #encodeMetadataHeader(ByteBufAllocator, byte, int)
  public static void encodeAndAddMetadata(
      CompositeByteBuf compositeMetaData,
      ByteBufAllocator allocator,
      WellKnownMimeType knownMimeType,
      ByteBuf metadata) {
    compositeMetaData.addComponents(
        true,
        encodeMetadataHeader(allocator, knownMimeType.getIdentifier(), metadata.readableBytes()),
        metadata);
  }

  /**
   * Encode a new sub-metadata information into a composite metadata {@link CompositeByteBuf
   * buffer}.
   *
   * @param compositeMetaData the buffer that will hold all composite metadata information.
   * @param allocator the {@link ByteBufAllocator} to use to create intermediate buffers as needed.
   * @param unknownCompressedMimeType the id of the {@link
   *     WellKnownMimeType#UNKNOWN_RESERVED_MIME_TYPE} to encode.
   * @param metadata the metadata value to encode.
   */
  // see #encodeMetadataHeader(ByteBufAllocator, byte, int)
  static void encodeAndAddMetadata(
      CompositeByteBuf compositeMetaData,
      ByteBufAllocator allocator,
      byte unknownCompressedMimeType,
      ByteBuf metadata) {
    compositeMetaData.addComponents(
        true,
        encodeMetadataHeader(allocator, unknownCompressedMimeType, metadata.readableBytes()),
        metadata);
  }

  // === ENTRY ===

  /**
   * An entry in a Composite Metadata, which exposes the {@link #getMimeType() mime type} and {@link
   * ByteBuf} {@link #getMetadata() content} of the metadata entry.
   *
   * <p>There is one case where the entry cannot really be used other than by forwarding it to
   * another client: when the mime type is represented as a compressed {@code byte} id, but said id
   * is only identified as "reserved" in the current implementation ({@link
   * WellKnownMimeType#UNKNOWN_RESERVED_MIME_TYPE}). In that case, the corresponding {@link Entry}
   * should reflect that by having a {@code null} {@link #getMimeType()} along a positive {@link
   * #getMimeId()}.
   *
   * <p>Non-null {@link #getMimeType()} along with positive {@link #getMimeId()} denote a compressed
   * mime metadata entry, whereas the same with a negative {@link #getMimeId()} would denote a
   * custom mime type metadata entry.
   *
   * <p>In all three cases, the {@link #getMetadata()} expose the content of the metadata entry as a
   * raw {@link ByteBuf}.
   */
  public static class Entry {

    /**
     * Create an {@link Entry} from a {@link WellKnownMimeType}. This will be encoded in a
     * compressed format that uses the {@link WellKnownMimeType#getIdentifier() mime identifier}.
     *
     * @param mimeType the {@link WellKnownMimeType} to use for the entry
     * @param metadataContentBuffer the content {@link ByteBuf} to use for the entry
     * @return the new entry
     */
    public static Entry wellKnownMime(WellKnownMimeType mimeType, ByteBuf metadataContentBuffer) {
      return new Entry(mimeType.getMime(), mimeType.getIdentifier(), metadataContentBuffer);
    }

    /**
     * Create an {@link Entry} from a custom mime type represented as an US-ASCII only {@link
     * String}. The whole literal mime type will thus be encoded.
     *
     * @param mimeType the custom mime type {@link String}
     * @param metadataContentBuffer the content {@link ByteBuf} to use for the entry
     * @return the new entry
     */
    public static Entry customMime(String mimeType, ByteBuf metadataContentBuffer) {
      return new Entry(mimeType, (byte) -1, metadataContentBuffer);
    }

    /**
     * Create an {@link Entry} from an unrecognized yet valid "well-known" mime type, ie. a {@code
     * byte} that would map to {@link WellKnownMimeType#UNKNOWN_RESERVED_MIME_TYPE}. Prefer using
     * {@link #wellKnownMime(WellKnownMimeType, ByteBuf)} if the mime code is recognizable by this
     * client.
     *
     * <p>This case would usually be encountered when decoding a composite metadata entry from a
     * remote that uses a more recent version of the {@link WellKnownMimeType} extension, and this
     * method can be useful to create an unprocessed entry in such a case, ensuring no information
     * is lost when forwarding frames.
     *
     * @param mimeCode the reserved but unrecognized compressed mime type {@code byte}
     * @param metadataContentBuffer the content {@link ByteBuf} to use for the entry
     * @return the new entry
     * @see #wellKnownMime(WellKnownMimeType, ByteBuf)
     */
    public static Entry rawCompressedMime(byte mimeCode, ByteBuf metadataContentBuffer) {
      return new Entry(null, mimeCode, metadataContentBuffer);
    }

    /**
     * Incrementally decode the next metadata entry from a {@link ByteBuf} into an {@link Entry}.
     * This is only possible on frame types used to initiate interactions, if the SETUP metadata
     * mime type was {@link WellKnownMimeType#MESSAGE_RSOCKET_COMPOSITE_METADATA}.
     *
     * <p>Each entry {@link ByteBuf} is a {@link ByteBuf#readSlice(int) slice} of the original
     * buffer that can also be {@link ByteBuf#readRetainedSlice(int) retained} if needed.
     *
     * @param buffer the buffer to decode
     * @param retainMetadataSlices should each slide be retained when read from the original buffer?
     * @return the decoded {@link Entry}
     */
    static Entry decode(ByteBuf buffer, boolean retainMetadataSlices) {
      ByteBuf[] entry = decodeMimeAndContentBuffers(buffer, retainMetadataSlices);
      if (entry == METADATA_MALFORMED) {
        throw new IllegalArgumentException(
            "composite metadata entry buffer is too short to contain proper entry");
      }
      if (entry == METADATA_BUFFERS_DONE) {
        return null;
      }

      ByteBuf encodedHeader = entry[0];
      ByteBuf metadataContent = entry[1];

      // the flyweight already validated the size of the buffer,
      // this is only to distinguish id vs custom type
      if (encodedHeader.readableBytes() == 1) {
        // id
        byte id = decodeMimeIdFromMimeBuffer(encodedHeader);
        WellKnownMimeType wkn = WellKnownMimeType.fromId(id);
        if (wkn == WellKnownMimeType.UNPARSEABLE_MIME_TYPE) {
          // should not happen due to flyweight decodeMimeAndContentBuffer's own guard
          throw new IllegalStateException(
              "composite metadata entry parsing failed on compressed mime id " + id);
        }
        if (wkn == WellKnownMimeType.UNKNOWN_RESERVED_MIME_TYPE) {
          return new Entry(null, id, metadataContent);
        }
        return new Entry(wkn.getMime(), wkn.getIdentifier(), metadataContent);
      } else {
        CharSequence customMimeCharSequence = decodeMimeTypeFromMimeBuffer(encodedHeader);
        if (customMimeCharSequence == null) {
          // should not happen due to flyweight decodeMimeAndContentBuffer's own guard
          throw new IllegalArgumentException(
              "composite metadata entry parsing failed on custom type");
        }
        return new Entry(customMimeCharSequence.toString(), (byte) -1, metadataContent);
      }
    }

    /**
     * Decode all the metadata entries from a {@link ByteBuf} into a {@link List} of {@link Entry}.
     * This is only possible on frame types used to initiate interactions, if the SETUP metadata
     * mime type was {@link WellKnownMimeType#MESSAGE_RSOCKET_COMPOSITE_METADATA}.
     *
     * <p>Each entry's {@link Entry#getMetadata() content} is a {@link ByteBuf#readSlice(int) slice}
     * of the original buffer that can also be {@link ByteBuf#readRetainedSlice(int) retained} if
     * needed.
     *
     * <p>The buffer is assumed to contain just enough bytes to represent one or more entries (mime
     * type compressed or not). The decoding stops when the buffer reaches 0 readable bytes, and
     * fails if it contains bytes but not enough to correctly decode an entry.
     *
     * @param buffer the buffer to decode
     * @param retainMetadataSlices should each slide be retained when read from the original buffer?
     * @return the {@link List} of decoded {@link Entry}
     */
    static List<Entry> decodeAll(ByteBuf buffer, boolean retainMetadataSlices) {
      List<Entry> list = new ArrayList<>();
      Entry nextEntry = decode(buffer, retainMetadataSlices);
      while (nextEntry != null) {
        list.add(nextEntry);
        nextEntry = decode(buffer, retainMetadataSlices);
      }
      return list;
    }

    private final String mimeString;
    private final byte mimeCode;
    private final ByteBuf content;

    public Entry(@Nullable String mimeString, byte mimeCode, ByteBuf content) {
      this.mimeString = mimeString;
      this.mimeCode = mimeCode;
      this.content = content;
    }

    /**
     * Returns the mime type {@link String} representation if there is one.
     *
     * <p>A {@code null} value should only occur with a positive {@link #getMimeId()}, denoting an
     * entry that is compressed but unparseable (see {@link #rawCompressedMime(byte, ByteBuf)}).
     *
     * @return the mime type for this entry, or null
     */
    @Nullable
    public String getMimeType() {
      return this.mimeString;
    }

    /** @return the compressed mime id byte if relevant (0-127), or -1 if not */
    public byte getMimeId() {
      return this.mimeCode;
    }

    /** @return the metadata content of this entry */
    public ByteBuf getMetadata() {
      return this.content;
    }

    /**
     * Encode this {@link Entry} into a {@link CompositeByteBuf} representing a composite metadata.
     * This buffer may already hold components for previous {@link Entry entries}.
     *
     * @param compositeByteBuf the {@link CompositeByteBuf} to hold the components of the whole
     *     composite metadata
     * @param byteBufAllocator the {@link ByteBufAllocator} to use to allocate new buffers as needed
     */
    public void encodeInto(CompositeByteBuf compositeByteBuf, ByteBufAllocator byteBufAllocator) {
      if (this.mimeCode >= 0) {
        encodeAndAddMetadata(compositeByteBuf, byteBufAllocator, this.mimeCode, this.content);
      } else {
        encodeAndAddMetadata(compositeByteBuf, byteBufAllocator, this.mimeString, this.content);
      }
    }
  }
}
