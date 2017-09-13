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

import io.rsocket.Frame;
import io.rsocket.Payload;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;

/**
 * An implementation of {@link Payload}. This implementation is <b>not</b> thread-safe, and hence
 * any method can not be invoked concurrently.
 */
public class PayloadImpl implements Payload {

  public static final PayloadImpl EMPTY =
      new PayloadImpl(Frame.NULL_BYTEBUFFER, Frame.NULL_BYTEBUFFER, false);

  private final ByteBuffer data;
  private final ByteBuffer metadata;
  private final int dataStartPosition;
  private final int metadataStartPosition;
  private final boolean reusable;

  public PayloadImpl(Frame frame) {
    this(frame.getData(), frame.hasMetadata() ? frame.getMetadata() : null);
  }

  public PayloadImpl(String data) {
    this(data, Charset.defaultCharset());
  }

  public PayloadImpl(String data, @Nullable String metadata) {
    this(data, StandardCharsets.UTF_8, metadata, StandardCharsets.UTF_8);
  }

  public PayloadImpl(String data, Charset dataCharset) {
    this(dataCharset.encode(data), null);
  }

  public PayloadImpl(
      String data, Charset dataCharset, @Nullable String metadata, Charset metaDataCharset) {
    this(dataCharset.encode(data), metadata == null ? null : metaDataCharset.encode(metadata));
  }

  public PayloadImpl(byte[] data) {
    this(ByteBuffer.wrap(data), Frame.NULL_BYTEBUFFER);
  }

  public PayloadImpl(byte[] data, @Nullable byte[] metadata) {
    this(ByteBuffer.wrap(data), metadata == null ? null : ByteBuffer.wrap(metadata));
  }

  public PayloadImpl(ByteBuffer data) {
    this(data, Frame.NULL_BYTEBUFFER);
  }

  public PayloadImpl(ByteBuffer data, @Nullable ByteBuffer metadata) {
    this(data, metadata, true);
  }

  public PayloadImpl(ByteBuffer data, ByteBuffer metadata, boolean reusable) {
    this.data = data;
    this.metadata = metadata;
    this.reusable = reusable;
    this.dataStartPosition = reusable ? this.data.position() : 0;
    this.metadataStartPosition = (reusable && metadata != null) ? this.metadata.position() : 0;
  }

  @Override
  public ByteBuffer getData() {
    if (reusable) {
      data.position(dataStartPosition);
    }
    return data;
  }

  @Override
  public ByteBuffer getMetadata() {
    if (metadata == null) {
      return Frame.NULL_BYTEBUFFER;
    }
    if (reusable) {
      metadata.position(metadataStartPosition);
    }
    return metadata;
  }

  @Override
  public boolean hasMetadata() {
    return metadata != null;
  }

  /**
   * Static factory method for a text payload. Mainly looks better than "new PayloadImpl(data)"
   *
   * @param data the data of the payload.
   * @return a payload.
   */
  public static Payload textPayload(String data) {
    return new PayloadImpl(data);
  }

  /**
   * Static factory method for a text payload. Mainly looks better than "new PayloadImpl(data,
   * metadata)"
   *
   * @param data the data of the payload.
   * @param metadata the metadata for the payload.
   * @return a payload.
   */
  public static Payload textPayload(String data, @Nullable String metadata) {
    return new PayloadImpl(data, metadata);
  }
}
