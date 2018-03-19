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

package io.rsocket.framing;

import static io.rsocket.framing.LengthUtils.getLengthAsUnsignedMedium;
import static io.rsocket.util.NumberUtils.MEDIUM_BYTES;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler.Handle;
import java.util.Objects;
import reactor.util.annotation.Nullable;

/**
 * An abstract implementation of {@link MetadataAndDataFrame} that enables recycling for
 * performance.
 *
 * @param <SELF> the implementing type
 * @see io.netty.util.Recycler
 * @see <a
 *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#metadata-optional-header">Frame
 *     Metadata and Data</a>
 */
abstract class AbstractRecyclableMetadataAndDataFrame<
        SELF extends AbstractRecyclableMetadataAndDataFrame<SELF>>
    extends AbstractRecyclableFrame<SELF> implements MetadataAndDataFrame {

  private static final int FLAG_METADATA = 1 << 8;

  AbstractRecyclableMetadataAndDataFrame(Handle<SELF> handle) {
    super(handle);
  }

  /**
   * Appends data to the {@link ByteBuf}.
   *
   * @param byteBuf the {@link ByteBuf} to append to
   * @param data the data to append
   * @return the {@link ByteBuf} with data appended to it
   * @throws NullPointerException if {@code byteBuf} is {@code null}
   */
  static ByteBuf appendData(ByteBuf byteBuf, @Nullable ByteBuf data) {
    Objects.requireNonNull(byteBuf, "byteBuf must not be null");

    if (data == null) {
      return byteBuf;
    }

    return Unpooled.wrappedBuffer(byteBuf, data.retain());
  }

  /**
   * Appends metadata to the {@link ByteBuf}.
   *
   * @param byteBufAllocator the {@link ByteBufAllocator} to use
   * @param byteBuf the {@link ByteBuf} to append to
   * @param metadata the metadata to append
   * @return the {@link ByteBuf} with metadata appended to it
   * @throws NullPointerException if {@code byteBuf} is {@code null}
   */
  static ByteBuf appendMetadata(
      ByteBufAllocator byteBufAllocator, ByteBuf byteBuf, @Nullable ByteBuf metadata) {
    Objects.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    Objects.requireNonNull(byteBuf, "byteBuf must not be null");

    if (metadata == null) {
      return byteBuf.writeMedium(0);
    }

    ByteBuf frame =
        setFlag(byteBuf, FLAG_METADATA).writeMedium(getLengthAsUnsignedMedium(metadata));
    frame = Unpooled.wrappedBuffer(frame, metadata.retain(), byteBufAllocator.buffer());

    return frame;
  }

  /**
   * Returns the data.
   *
   * @param metadataLengthOffset the offset that the metadataLength starts at, relative to start of
   *     the {@link ByteBuf}
   * @return the data
   */
  final ByteBuf getData(int metadataLengthOffset) {
    int dataOffset = getDataOffset(metadataLengthOffset);
    ByteBuf byteBuf = getByteBuf();
    return byteBuf.slice(dataOffset, byteBuf.readableBytes() - dataOffset).asReadOnly();
  }

  /**
   * Returns the metadata.
   *
   * @param metadataLengthOffset the offset that the metadataLength starts at, relative to start of
   *     the {@link ByteBuf}
   * @return the data
   */
  final @Nullable ByteBuf getMetadata(int metadataLengthOffset) {
    if (!isFlagSet(FLAG_METADATA)) {
      return null;
    }

    ByteBuf byteBuf = getByteBuf();
    return byteBuf
        .slice(getMetadataOffset(metadataLengthOffset), getMetadataLength(metadataLengthOffset))
        .asReadOnly();
  }

  private static int getMetadataOffset(int metadataLengthOffset) {
    return metadataLengthOffset + MEDIUM_BYTES;
  }

  private int getDataOffset(int metadataLengthOffset) {
    return getMetadataOffset(metadataLengthOffset) + getMetadataLength(metadataLengthOffset);
  }

  private int getMetadataLength(int metadataLengthOffset) {
    return getByteBuf().getUnsignedMedium(metadataLengthOffset);
  }
}
