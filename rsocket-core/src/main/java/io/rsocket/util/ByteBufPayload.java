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
import io.netty.util.Recycler;
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

  private final Recycler.Handle<ByteBufPayload> handle;
  private ByteBuf data;
  private ByteBuf metadata;

  private ByteBufPayload(final Recycler.Handle<ByteBufPayload> handle) {
    this.handle = handle;
  }

  @Override
  public boolean hasMetadata() {
    return metadata != null;
  }

  @Override
  public ByteBuf sliceMetadata() {
    return metadata == null ? Unpooled.EMPTY_BUFFER : metadata.slice();
  }

  @Override
  public ByteBuf sliceData() {
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
    data.touch();
    if (metadata != null) {
      metadata.touch();
    }
    return this;
  }

  @Override
  public ByteBufPayload touch(Object hint) {
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
    payload.setRefCnt(1);
    payload.data = data;
    payload.metadata = metadata;
    return payload;
  }

  public static Payload create(Payload payload) {
    return create(payload.sliceData(), payload.hasMetadata() ? payload.sliceMetadata() : null);
  }
}
