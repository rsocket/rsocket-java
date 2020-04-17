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
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;

/**
 * An implementation of {@link Payload}. This implementation is <b>not</b> thread-safe, and hence
 * any method can not be invoked concurrently.
 */
public final class DefaultPayload implements Payload {
  public static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocateDirect(0);

  private final ByteBuffer data;
  private final ByteBuffer metadata;

  private DefaultPayload(ByteBuffer data, @Nullable ByteBuffer metadata) {
    this.data = data;
    this.metadata = metadata;
  }

  /**
   * Static factory method for a text payload. Mainly looks better than "new DefaultPayload(data)"
   *
   * @param data the data of the payload.
   * @return a payload.
   */
  public static Payload create(CharSequence data) {
    return create(StandardCharsets.UTF_8.encode(CharBuffer.wrap(data)), null);
  }

  /**
   * Static factory method for a text payload. Mainly looks better than "new DefaultPayload(data,
   * metadata)"
   *
   * @param data the data of the payload.
   * @param metadata the metadata for the payload.
   * @return a payload.
   */
  public static Payload create(CharSequence data, @Nullable CharSequence metadata) {
    return create(
        StandardCharsets.UTF_8.encode(CharBuffer.wrap(data)),
        metadata == null ? null : StandardCharsets.UTF_8.encode(CharBuffer.wrap(metadata)));
  }

  public static Payload create(CharSequence data, Charset dataCharset) {
    return create(dataCharset.encode(CharBuffer.wrap(data)), null);
  }

  public static Payload create(
      CharSequence data,
      Charset dataCharset,
      @Nullable CharSequence metadata,
      Charset metadataCharset) {
    return create(
        dataCharset.encode(CharBuffer.wrap(data)),
        metadata == null ? null : metadataCharset.encode(CharBuffer.wrap(metadata)));
  }

  public static Payload create(byte[] data) {
    return create(ByteBuffer.wrap(data), null);
  }

  public static Payload create(byte[] data, @Nullable byte[] metadata) {
    return create(ByteBuffer.wrap(data), metadata == null ? null : ByteBuffer.wrap(metadata));
  }

  public static Payload create(ByteBuffer data) {
    return create(data, null);
  }

  public static Payload create(ByteBuffer data, @Nullable ByteBuffer metadata) {
    return new DefaultPayload(data, metadata);
  }

  public static Payload create(ByteBuf data) {
    return create(data, null);
  }

  public static Payload create(ByteBuf data, @Nullable ByteBuf metadata) {
    return create(data.nioBuffer(), metadata == null ? null : metadata.nioBuffer());
  }

  public static Payload create(Payload payload) {
    return create(
        Unpooled.copiedBuffer(payload.sliceData()),
        payload.hasMetadata() ? Unpooled.copiedBuffer(payload.sliceMetadata()) : null);
  }

  @Override
  public boolean hasMetadata() {
    return metadata != null && metadata.remaining() > 0;
  }

  @Override
  public ByteBuf sliceMetadata() {
    return metadata == null ? Unpooled.EMPTY_BUFFER : Unpooled.wrappedBuffer(metadata);
  }

  @Override
  public ByteBuf sliceData() {
    return Unpooled.wrappedBuffer(data);
  }

  @Override
  public ByteBuffer getMetadata() {
    return metadata == null ? DefaultPayload.EMPTY_BUFFER : metadata.duplicate();
  }

  @Override
  public ByteBuffer getData() {
    return data.duplicate();
  }

  @Override
  public ByteBuf data() {
    return sliceData();
  }

  @Override
  public ByteBuf metadata() {
    return sliceMetadata();
  }

  @Override
  public int refCnt() {
    return 1;
  }

  @Override
  public DefaultPayload retain() {
    return this;
  }

  @Override
  public DefaultPayload retain(int increment) {
    return this;
  }

  @Override
  public DefaultPayload touch() {
    return this;
  }

  @Override
  public DefaultPayload touch(Object hint) {
    return this;
  }

  @Override
  public boolean release() {
    return false;
  }

  @Override
  public boolean release(int decrement) {
    return false;
  }
}
