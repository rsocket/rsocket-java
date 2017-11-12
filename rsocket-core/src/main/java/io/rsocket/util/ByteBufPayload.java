/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import io.rsocket.Payload;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;

public final class ByteBufPayload extends AbstractReferenceCounted implements Payload {
  private final ByteBuf data;
  private final ByteBuf metadata;

  private ByteBufPayload(ByteBuf data) {
    this.data = data.asReadOnly();
    this.metadata = null;
  }

  private ByteBufPayload(ByteBuf data, @Nullable ByteBuf metadata) {
    this.data = data.asReadOnly();
    this.metadata = metadata == null ? null : metadata.asReadOnly();
  }

  @Override
  public boolean hasMetadata() {
    return metadata != null;
  }

  @Override
  public ByteBuf sliceMetadata() {
    return metadata == null ? Unpooled.EMPTY_BUFFER : metadata.duplicate();
  }

  @Override
  public ByteBuf sliceData() {
    return data.duplicate();
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
    if (metadata != null) {
      metadata.release();
    }
  }

  /**
   * Static factory method for a text payload. Mainly looks better than "new ByteBufPayload(data)"
   *
   * @param data the data of the payload.
   * @return a payload.
   */
  public static Payload create(String data) {
    return new ByteBufPayload(ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, data));
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
    return new ByteBufPayload(
        ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, data),
        metadata == null ? null : ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, metadata)
    );
  }

  public static Payload create(CharSequence data, Charset dataCharset) {
    return new ByteBufPayload(ByteBufUtil.encodeString(ByteBufAllocator.DEFAULT, CharBuffer.wrap(data), dataCharset));
  }

  public static Payload create(CharSequence data, Charset dataCharset, @Nullable CharSequence metadata, Charset metadataCharset) {
    return new ByteBufPayload(
        ByteBufUtil.encodeString(ByteBufAllocator.DEFAULT, CharBuffer.wrap(data), dataCharset),
        metadata == null ? null : ByteBufUtil.encodeString(ByteBufAllocator.DEFAULT, CharBuffer.wrap(metadata), metadataCharset)
    );
  }

  public static Payload create(byte[] data) {
    return new ByteBufPayload(Unpooled.wrappedBuffer(data));
  }

  public static Payload create(byte[] data, @Nullable byte[] metadata) {
    return new ByteBufPayload(Unpooled.wrappedBuffer(data), metadata == null ? null : Unpooled.wrappedBuffer(metadata));
  }

  public static Payload create(ByteBuffer data) {
    return new ByteBufPayload(Unpooled.wrappedBuffer(data));
  }

  public static Payload create(ByteBuffer data, @Nullable ByteBuffer metadata) {
    return new ByteBufPayload(Unpooled.wrappedBuffer(data), metadata == null ? null : Unpooled.wrappedBuffer(metadata));
  }

  public static Payload create(ByteBuf data) {
    return new ByteBufPayload(data);
  }

  public static Payload create(ByteBuf data, @Nullable ByteBuf metadata) {
    return new ByteBufPayload(data, metadata);
  }

  public static Payload create(Payload payload) {
    return new ByteBufPayload(payload.sliceData(), payload.hasMetadata() ? payload.sliceMetadata() : null);
  }
}
