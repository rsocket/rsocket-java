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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler.Handle;
import java.util.Objects;
import reactor.util.annotation.Nullable;

/**
 * An abstract implementation of {@link MetadataFrame} that enables recycling for performance.
 *
 * @param <SELF> the implementing type
 * @see io.netty.util.Recycler
 * @see <a
 *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#metadata-optional-header">Frame
 *     Metadata</a>
 */
abstract class AbstractRecyclableMetadataFrame<SELF extends AbstractRecyclableMetadataFrame<SELF>>
    extends AbstractRecyclableFrame<SELF> implements MetadataFrame {

  private static final int FLAG_METADATA = 1 << 8;

  AbstractRecyclableMetadataFrame(Handle<SELF> handle) {
    super(handle);
  }

  /**
   * Appends metadata to the {@link ByteBuf}.
   *
   * @param byteBuf the {@link ByteBuf} to append to
   * @param metadata the metadata to append
   * @return the {@link ByteBuf} with metadata appended to it
   * @throws NullPointerException if {@code byteBuf} is {@code null}
   */
  static ByteBuf appendMetadata(ByteBuf byteBuf, @Nullable ByteBuf metadata) {
    Objects.requireNonNull(byteBuf, "byteBuf must not be null");

    if (metadata == null) {
      return byteBuf;
    }

    setFlag(byteBuf, FLAG_METADATA);

    return Unpooled.wrappedBuffer(byteBuf, metadata.retain());
  }

  /**
   * Returns the metadata.
   *
   * @param metadataOffset the offset that the metadata starts at, relative to start of the {@link
   *     ByteBuf}
   * @return the metadata or {@code null} if the metadata flag is not set
   */
  final @Nullable ByteBuf getMetadata(int metadataOffset) {
    if (!isFlagSet(FLAG_METADATA)) {
      return null;
    }

    ByteBuf byteBuf = getByteBuf();
    return byteBuf.slice(metadataOffset, byteBuf.readableBytes() - metadataOffset).asReadOnly();
  }
}
