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

package io.rsocket.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.rsocket.Payload;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import javax.annotation.Nullable;

public final class ByteBufPayload extends AbstractReferenceCounted implements Payload {
  private static final Recycler<ByteBufPayload> RECYCLER =
      new Recycler<ByteBufPayload>() {
        protected ByteBufPayload newObject(Handle<ByteBufPayload> handle) {
          return new ByteBufPayload(handle);
        }
      };

  private final Handle<ByteBufPayload> handle;
  private ByteBuf data;
  private ByteBuf metadata;

  private ByteBufPayload(final Handle<ByteBufPayload> handle) {
    this.handle = handle;
  }

  /**
   * Static factory method for a text payload. Mainly looks better than "new ByteBufPayload(data)"
   *
   * @param data the data of the payload.
   * @return a payload.
   */
  public static Payload create(String data) {
    return create(ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, data), null);
  }

  /**
   * Static factory method for a text payload. Mainly looks better than "new ByteBufPayload(data,
   * metadata)"
   *
   * @param data the data of the payload.
   * @param metadata the metadata for the payload.
   * @return a payload.
   */
  public static Payload create(String data, @Nullable String metadata) {
    return create(
        ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, data),
        metadata == null ? null : ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, metadata));
  }

  public static Payload create(CharSequence data, Charset dataCharset) {
    return create(
        ByteBufUtil.encodeString(ByteBufAllocator.DEFAULT, CharBuffer.wrap(data), dataCharset),
        null);
  }

  public static Payload create(
      CharSequence data,
      Charset dataCharset,
      @Nullable CharSequence metadata,
      Charset metadataCharset) {
    return create(
        ByteBufUtil.encodeString(ByteBufAllocator.DEFAULT, CharBuffer.wrap(data), dataCharset),
        metadata == null
            ? null
            : ByteBufUtil.encodeString(
                ByteBufAllocator.DEFAULT, CharBuffer.wrap(metadata), metadataCharset));
  }

  public static Payload create(byte[] data) {
    return create(Unpooled.wrappedBuffer(data), null);
  }

  public static Payload create(byte[] data, @Nullable byte[] metadata) {
    return create(
        Unpooled.wrappedBuffer(data), metadata == null ? null : Unpooled.wrappedBuffer(metadata));
  }

  public static Payload create(ByteBuffer data) {
    return create(Unpooled.wrappedBuffer(data), null);
  }

  public static Payload create(ByteBuffer data, @Nullable ByteBuffer metadata) {
    return create(
        Unpooled.wrappedBuffer(data), metadata == null ? null : Unpooled.wrappedBuffer(metadata));
  }

  public static Payload create(ByteBuf data) {
    return create(data, null);
  }

  public static Payload create(ByteBuf data, @Nullable ByteBuf metadata) {
    ByteBufPayload payload = RECYCLER.get();
    payload.data = data;
    payload.metadata = metadata;
    // unsure data and metadata is set before refCnt change
    payload.setRefCnt(1);
    return payload;
  }

  public static Payload create(Payload payload) {
    return create(
        payload.sliceData().retain(),
        payload.hasMetadata() ? payload.sliceMetadata().retain() : null);
  }

  @Override
  public boolean hasMetadata() {
    ensureAccessible();
    return metadata != null && metadata.isReadable();
  }

  @Override
  public ByteBuf sliceMetadata() {
    ensureAccessible();
    return metadata == null ? Unpooled.EMPTY_BUFFER : metadata.slice();
  }

  @Override
  public ByteBuf data() {
    ensureAccessible();
    return data;
  }

  @Override
  public ByteBuf metadata() {
    ensureAccessible();
    return metadata == null ? Unpooled.EMPTY_BUFFER : metadata;
  }

  @Override
  public ByteBuf sliceData() {
    ensureAccessible();
    return data.slice();
  }

  @Override
  public ByteBufPayload retain() {
    super.retain();
    return this;
  }

  @Override
  public ByteBufPayload retain(int increment) {
    super.retain(increment);
    return this;
  }

  @Override
  public ByteBufPayload touch() {
    ensureAccessible();
    data.touch();
    if (metadata != null) {
      metadata.touch();
    }
    return this;
  }

  @Override
  public ByteBufPayload touch(Object hint) {
    ensureAccessible();
    data.touch(hint);
    if (metadata != null) {
      metadata.touch(hint);
    }
    return this;
  }

  @Override
  protected void deallocate() {
    data.release();
    data = null;
    if (metadata != null) {
      metadata.release();
      metadata = null;
    }
    handle.recycle(this);
  }

  /**
   * Should be called by every method that tries to access the buffers content to check if the
   * buffer was released before.
   */
  void ensureAccessible() {
    if (!isAccessible()) {
      throw new IllegalReferenceCountException(0);
    }
  }

  /**
   * Used internally by {@link ByteBufPayload#ensureAccessible()} to try to guard against using the
   * buffer after it was released (best-effort).
   */
  boolean isAccessible() {
    return refCnt() != 0;
  }
}
