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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import reactor.util.annotation.Nullable;

/**
 * A flyweight class that can be used to encode/decode composite metadata information to/from {@link
 * ByteBuf}. This is intended for low-level efficient manipulation of such buffers. See {@link
 * CompositeMetadata} for an Iterator-like approach to decoding entries.
 *
 * @deprecated in favor of {@link CompositeMetadataCodec}
 */
@Deprecated
public class CompositeMetadataFlyweight {

  private CompositeMetadataFlyweight() {}

  public static int computeNextEntryIndex(
      int currentEntryIndex, ByteBuf headerSlice, ByteBuf contentSlice) {
    return CompositeMetadataCodec.computeNextEntryIndex(
        currentEntryIndex, headerSlice, contentSlice);
  }

  /**
   * Decode the next metadata entry (a mime header + content pair of {@link ByteBuf}) from a {@link
   * ByteBuf} that contains at least enough bytes for one more such entry. These buffers are
   * actually slices of the full metadata buffer, and this method doesn't move the full metadata
   * buffer's {@link ByteBuf#readerIndex()}. As such, it requires the user to provide an {@code
   * index} to read from. The next index is computed by calling {@link #computeNextEntryIndex(int,
   * ByteBuf, ByteBuf)}. Size of the first buffer (the "header buffer") drives which decoding method
   * should be further applied to it.
   *
   * <p>The header buffer is either:
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
   * @param compositeMetadata the source {@link ByteBuf} that originally contains one or more
   *     metadata entries
   * @param entryIndex the {@link ByteBuf#readerIndex()} to start decoding from. original reader
   *     index is kept on the source buffer
   * @param retainSlices should produced metadata entry buffers {@link ByteBuf#slice() slices} be
   *     {@link ByteBuf#retainedSlice() retained}?
   * @return a {@link ByteBuf} array of length 2 containing the mime header buffer
   *     <strong>slice</strong> and the content buffer <strong>slice</strong>, or one of the
   *     zero-length error constant arrays
   */
  public static ByteBuf[] decodeMimeAndContentBuffersSlices(
      ByteBuf compositeMetadata, int entryIndex, boolean retainSlices) {
    return CompositeMetadataCodec.decodeMimeAndContentBuffersSlices(
        compositeMetadata, entryIndex, retainSlices);
  }

  /**
   * Decode a {@code byte} compressed mime id from a {@link ByteBuf}, assuming said buffer properly
   * contains such an id.
   *
   * <p>The buffer must have exactly one readable byte, which is assumed to have been tested for
   * mime id encoding via the {@link CompositeMetadataCodec#STREAM_METADATA_KNOWN_MASK} mask ({@code
   * firstByte & STREAM_METADATA_KNOWN_MASK) == STREAM_METADATA_KNOWN_MASK}).
   *
   * <p>If there is no readable byte, the negative identifier of {@link
   * WellKnownMimeType#UNPARSEABLE_MIME_TYPE} is returned.
   *
   * @param mimeBuffer the buffer that should next contain the compressed mime id byte
   * @return the compressed mime id, between 0 and 127, or a negative id if the input is invalid
   * @see #decodeMimeTypeFromMimeBuffer(ByteBuf)
   */
  public static byte decodeMimeIdFromMimeBuffer(ByteBuf mimeBuffer) {
    return CompositeMetadataCodec.decodeMimeIdFromMimeBuffer(mimeBuffer);
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
    return CompositeMetadataCodec.decodeMimeTypeFromMimeBuffer(flyweightMimeBuffer);
  }

  /**
   * Encode a new sub-metadata information into a composite metadata {@link CompositeByteBuf
   * buffer}, without checking if the {@link String} can be matched with a well known compressable
   * mime type. Prefer using this method and {@link #encodeAndAddMetadata(CompositeByteBuf,
   * ByteBufAllocator, WellKnownMimeType, ByteBuf)} if you know in advance whether or not the mime
   * is well known. Otherwise use {@link #encodeAndAddMetadataWithCompression(CompositeByteBuf,
   * ByteBufAllocator, String, ByteBuf)}
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
    CompositeMetadataCodec.encodeAndAddMetadata(
        compositeMetaData, allocator, customMimeType, metadata);
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
    CompositeMetadataCodec.encodeAndAddMetadata(
        compositeMetaData, allocator, knownMimeType, metadata);
  }

  /**
   * Encode a new sub-metadata information into a composite metadata {@link CompositeByteBuf
   * buffer}, first verifying if the passed {@link String} matches a {@link WellKnownMimeType} (in
   * which case it will be encoded in a compressed fashion using the mime id of that type).
   *
   * <p>Prefer using {@link #encodeAndAddMetadata(CompositeByteBuf, ByteBufAllocator, String,
   * ByteBuf)} if you already know that the mime type is not a {@link WellKnownMimeType}.
   *
   * @param compositeMetaData the buffer that will hold all composite metadata information.
   * @param allocator the {@link ByteBufAllocator} to use to create intermediate buffers as needed.
   * @param mimeType the mime type to encode, as a {@link String}. well known mime types are
   *     compressed.
   * @param metadata the metadata value to encode.
   * @see #encodeAndAddMetadata(CompositeByteBuf, ByteBufAllocator, WellKnownMimeType, ByteBuf)
   */
  // see #encodeMetadataHeader(ByteBufAllocator, String, int)
  public static void encodeAndAddMetadataWithCompression(
      CompositeByteBuf compositeMetaData,
      ByteBufAllocator allocator,
      String mimeType,
      ByteBuf metadata) {
    CompositeMetadataCodec.encodeAndAddMetadataWithCompression(
        compositeMetaData, allocator, mimeType, metadata);
  }

  /**
   * Returns whether there is another entry available at a given index
   *
   * @param compositeMetadata the buffer to inspect
   * @param entryIndex the index to check at
   * @return whether there is another entry available at a given index
   */
  public static boolean hasEntry(ByteBuf compositeMetadata, int entryIndex) {
    return CompositeMetadataCodec.hasEntry(compositeMetadata, entryIndex);
  }

  /**
   * Returns whether the header represents a well-known MIME type.
   *
   * @param header the header to inspect
   * @return whether the header represents a well-known MIME type
   */
  public static boolean isWellKnownMimeType(ByteBuf header) {
    return CompositeMetadataCodec.isWellKnownMimeType(header);
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
    CompositeMetadataCodec.encodeAndAddMetadata(
        compositeMetaData, allocator, unknownCompressedMimeType, metadata);
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
    return CompositeMetadataCodec.encodeMetadataHeader(allocator, customMime, metadataLength);
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
    return CompositeMetadataCodec.encodeMetadataHeader(allocator, mimeType, metadataLength);
  }
}
