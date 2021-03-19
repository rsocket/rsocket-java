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
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.CharsetUtil;
import java.util.ArrayList;
import java.util.List;

/**
 * Provides support for encoding and/or decoding the per-stream MIME type to use for payload data.
 *
 * <p>For more on the format of the metadata, see the <a
 * href="https://github.com/rsocket/rsocket/blob/master/Extensions/PerStreamDataMimeTypesDefinition.md">
 * Stream Data MIME Types</a> extension specification.
 *
 * @since 1.1.1
 */
public class MimeTypeMetadataCodec {
  private static final int STREAM_METADATA_KNOWN_MASK = 0x80; // 1000 0000
  private static final byte STREAM_METADATA_LENGTH_MASK = 0x7F; // 0111 1111

  private MimeTypeMetadataCodec() {}

  /**
   * Encode a {@link WellKnownMimeType} into a newly allocated {@link ByteBuf} and this can then be
   * decoded using {@link #decode(ByteBuf)}.
   */
  public static ByteBuf encode(ByteBufAllocator allocator, WellKnownMimeType mimeType) {
    return allocator.buffer(1, 1).writeByte(mimeType.getIdentifier() | STREAM_METADATA_KNOWN_MASK);
  }

  /**
   * Encode either a {@link WellKnownMimeType} or a custom mime type into a newly allocated {@link
   * ByteBuf}.
   *
   * @param allocator the {@link ByteBufAllocator} to use to create the buffer
   * @param mimeType mime type
   * @return the encoded mime type
   */
  public static ByteBuf encode(ByteBufAllocator allocator, String mimeType) {
    if (mimeType == null || mimeType.length() == 0) {
      throw new IllegalArgumentException("Mime type null or length is zero");
    }
    WellKnownMimeType wkn = WellKnownMimeType.fromString(mimeType);
    if (wkn == WellKnownMimeType.UNPARSEABLE_MIME_TYPE) {
      return encodeCustomMime(allocator, mimeType);
    } else {
      return encode(allocator, wkn);
    }
  }

  /**
   * Encode multiple {@link WellKnownMimeType} or custom mime type into a newly allocated {@link
   * ByteBuf}.
   *
   * @param allocator the {@link ByteBufAllocator} to use to create the buffer
   * @param mimeTypes mime types
   * @return the encoded mime types
   */
  public static ByteBuf encode(ByteBufAllocator allocator, List<String> mimeTypes) {
    if (mimeTypes == null || mimeTypes.size() == 0) {
      throw new IllegalArgumentException("Mime types empty");
    }
    CompositeByteBuf compositeMimeByteBuf = allocator.compositeBuffer();
    for (String mimeType : mimeTypes) {
      compositeMimeByteBuf.addComponents(true, encode(allocator, mimeType));
    }
    return compositeMimeByteBuf;
  }

  /**
   * Encode a custom mime type into a newly allocated {@link ByteBuf}.
   *
   * <p>This larger representation encodes the mime type representation's length on a single byte,
   * then the representation itself.
   *
   * @param allocator the {@link ByteBufAllocator} to use to create the buffer
   * @param customMime a custom mime type to encode
   * @return the encoded mime
   */
  private static ByteBuf encodeCustomMime(ByteBufAllocator allocator, String customMime) {
    ByteBuf mime = allocator.buffer(1 + customMime.length());
    // reserve 1 byte for the customMime length
    // /!\ careful not to read that first byte, which is random at this point
    mime.writerIndex(1);

    // write the custom mime in UTF8 but validate it is all ASCII-compatible
    // (which produces the right result since ASCII chars are still encoded on 1 byte in UTF8)
    int customMimeLength = ByteBufUtil.writeUtf8(mime, customMime);
    if (!ByteBufUtil.isText(mime, mime.readerIndex() + 1, customMimeLength, CharsetUtil.US_ASCII)) {
      mime.release();
      throw new IllegalArgumentException("custom mime type must be US_ASCII characters only");
    }
    if (customMimeLength < 1 || customMimeLength > 128) {
      mime.release();
      throw new IllegalArgumentException(
          "custom mime type must have a strictly positive length that fits on 7 unsigned bits, ie 1-128");
    }
    mime.markWriterIndex();
    // go back to beginning and write the length
    // encoded length is one less than actual length, since 0 is never a valid length, which gives
    // wider representation range
    mime.writerIndex(0);
    mime.writeByte(customMimeLength - 1);

    // go back to post-mime type
    mime.resetWriterIndex();
    return mime;
  }

  /**
   * Decode mime types from a {@link ByteBuf} that contains at least enough bytes for one mime type.
   *
   * @return decoded mime types
   */
  public static List<String> decode(ByteBuf buf) {
    List<String> mimeTypes = new ArrayList<>();
    while (buf.isReadable()) {
      byte mimeIdOrLength = buf.readByte();
      if ((mimeIdOrLength & STREAM_METADATA_KNOWN_MASK) == STREAM_METADATA_KNOWN_MASK) {
        byte mimeIdentifier = (byte) (mimeIdOrLength & STREAM_METADATA_LENGTH_MASK);
        mimeTypes.add(WellKnownMimeType.fromIdentifier(mimeIdentifier).toString());
      } else {
        int mimeLen = Byte.toUnsignedInt(mimeIdOrLength) + 1;
        mimeTypes.add(buf.readCharSequence(mimeLen, CharsetUtil.US_ASCII).toString());
      }
    }
    return mimeTypes;
  }
}
