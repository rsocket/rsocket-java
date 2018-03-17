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

/* Generated SBE (Simple Binary Encoding) message codec */

package io.rsocket.aeron.internal.reactivestreams.messages;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

@javax.annotation.Generated(
  value = {"io.rsocket.aeron.internal.reactivestreams.messages.ConnectEncoder"}
)
@SuppressWarnings("all")
public class ConnectEncoder {
  public static final int BLOCK_LENGTH = 20;
  public static final int TEMPLATE_ID = 1;
  public static final int SCHEMA_ID = 1;
  public static final int SCHEMA_VERSION = 0;

  private final ConnectEncoder parentMessage = this;
  private MutableDirectBuffer buffer;
  protected int offset;
  protected int limit;
  protected int actingBlockLength;
  protected int actingVersion;

  public int sbeBlockLength() {
    return BLOCK_LENGTH;
  }

  public int sbeTemplateId() {
    return TEMPLATE_ID;
  }

  public int sbeSchemaId() {
    return SCHEMA_ID;
  }

  public int sbeSchemaVersion() {
    return SCHEMA_VERSION;
  }

  public String sbeSemanticType() {
    return "";
  }

  public int offset() {
    return offset;
  }

  public ConnectEncoder wrap(final MutableDirectBuffer buffer, final int offset) {
    this.buffer = buffer;
    this.offset = offset;
    limit(offset + BLOCK_LENGTH);

    return this;
  }

  public int encodedLength() {
    return limit - offset;
  }

  public int limit() {
    return limit;
  }

  public void limit(final int limit) {
    this.limit = limit;
  }

  public static long channelIdNullValue() {
    return -9223372036854775808L;
  }

  public static long channelIdMinValue() {
    return -9223372036854775807L;
  }

  public static long channelIdMaxValue() {
    return 9223372036854775807L;
  }

  public ConnectEncoder channelId(final long value) {
    buffer.putLong(offset + 0, value, java.nio.ByteOrder.LITTLE_ENDIAN);
    return this;
  }

  public static int sendingStreamIdNullValue() {
    return -2147483648;
  }

  public static int sendingStreamIdMinValue() {
    return -2147483647;
  }

  public static int sendingStreamIdMaxValue() {
    return 2147483647;
  }

  public ConnectEncoder sendingStreamId(final int value) {
    buffer.putInt(offset + 8, value, java.nio.ByteOrder.LITTLE_ENDIAN);
    return this;
  }

  public static int receivingStreamIdNullValue() {
    return -2147483648;
  }

  public static int receivingStreamIdMinValue() {
    return -2147483647;
  }

  public static int receivingStreamIdMaxValue() {
    return 2147483647;
  }

  public ConnectEncoder receivingStreamId(final int value) {
    buffer.putInt(offset + 12, value, java.nio.ByteOrder.LITTLE_ENDIAN);
    return this;
  }

  public static int clientSessionIdNullValue() {
    return -2147483648;
  }

  public static int clientSessionIdMinValue() {
    return -2147483647;
  }

  public static int clientSessionIdMaxValue() {
    return 2147483647;
  }

  public ConnectEncoder clientSessionId(final int value) {
    buffer.putInt(offset + 16, value, java.nio.ByteOrder.LITTLE_ENDIAN);
    return this;
  }

  public static int sendingChannelId() {
    return 5;
  }

  public static String sendingChannelCharacterEncoding() {
    return "UTF-8";
  }

  public static String sendingChannelMetaAttribute(final MetaAttribute metaAttribute) {
    switch (metaAttribute) {
      case EPOCH:
        return "unix";
      case TIME_UNIT:
        return "nanosecond";
      case SEMANTIC_TYPE:
        return "";
    }

    return "";
  }

  public static int sendingChannelHeaderLength() {
    return 4;
  }

  public ConnectEncoder putSendingChannel(
      final DirectBuffer src, final int srcOffset, final int length) {
    if (length > 1073741824) {
      throw new IllegalArgumentException("length > max value for type: " + length);
    }

    final int headerLength = 4;
    final int limit = parentMessage.limit();
    parentMessage.limit(limit + headerLength + length);
    buffer.putInt(limit, length, java.nio.ByteOrder.LITTLE_ENDIAN);
    buffer.putBytes(limit + headerLength, src, srcOffset, length);

    return this;
  }

  public ConnectEncoder putSendingChannel(final byte[] src, final int srcOffset, final int length) {
    if (length > 1073741824) {
      throw new IllegalArgumentException("length > max value for type: " + length);
    }

    final int headerLength = 4;
    final int limit = parentMessage.limit();
    parentMessage.limit(limit + headerLength + length);
    buffer.putInt(limit, length, java.nio.ByteOrder.LITTLE_ENDIAN);
    buffer.putBytes(limit + headerLength, src, srcOffset, length);

    return this;
  }

  public ConnectEncoder sendingChannel(final String value) {
    final byte[] bytes;
    try {
      bytes = value.getBytes("UTF-8");
    } catch (final java.io.UnsupportedEncodingException ex) {
      throw new RuntimeException(ex);
    }

    final int length = bytes.length;
    if (length > 1073741824) {
      throw new IllegalArgumentException("length > max value for type: " + length);
    }

    final int headerLength = 4;
    final int limit = parentMessage.limit();
    parentMessage.limit(limit + headerLength + length);
    buffer.putInt(limit, length, java.nio.ByteOrder.LITTLE_ENDIAN);
    buffer.putBytes(limit + headerLength, bytes, 0, length);

    return this;
  }

  public static int receivingChannelId() {
    return 6;
  }

  public static String receivingChannelCharacterEncoding() {
    return "UTF-8";
  }

  public static String receivingChannelMetaAttribute(final MetaAttribute metaAttribute) {
    switch (metaAttribute) {
      case EPOCH:
        return "unix";
      case TIME_UNIT:
        return "nanosecond";
      case SEMANTIC_TYPE:
        return "";
    }

    return "";
  }

  public static int receivingChannelHeaderLength() {
    return 4;
  }

  public ConnectEncoder putReceivingChannel(
      final DirectBuffer src, final int srcOffset, final int length) {
    if (length > 1073741824) {
      throw new IllegalArgumentException("length > max value for type: " + length);
    }

    final int headerLength = 4;
    final int limit = parentMessage.limit();
    parentMessage.limit(limit + headerLength + length);
    buffer.putInt(limit, length, java.nio.ByteOrder.LITTLE_ENDIAN);
    buffer.putBytes(limit + headerLength, src, srcOffset, length);

    return this;
  }

  public ConnectEncoder putReceivingChannel(
      final byte[] src, final int srcOffset, final int length) {
    if (length > 1073741824) {
      throw new IllegalArgumentException("length > max value for type: " + length);
    }

    final int headerLength = 4;
    final int limit = parentMessage.limit();
    parentMessage.limit(limit + headerLength + length);
    buffer.putInt(limit, length, java.nio.ByteOrder.LITTLE_ENDIAN);
    buffer.putBytes(limit + headerLength, src, srcOffset, length);

    return this;
  }

  public ConnectEncoder receivingChannel(final String value) {
    final byte[] bytes;
    try {
      bytes = value.getBytes("UTF-8");
    } catch (final java.io.UnsupportedEncodingException ex) {
      throw new RuntimeException(ex);
    }

    final int length = bytes.length;
    if (length > 1073741824) {
      throw new IllegalArgumentException("length > max value for type: " + length);
    }

    final int headerLength = 4;
    final int limit = parentMessage.limit();
    parentMessage.limit(limit + headerLength + length);
    buffer.putInt(limit, length, java.nio.ByteOrder.LITTLE_ENDIAN);
    buffer.putBytes(limit + headerLength, bytes, 0, length);

    return this;
  }

  public static int clientManagementChannelId() {
    return 6;
  }

  public static String clientManagementChannelCharacterEncoding() {
    return "UTF-8";
  }

  public static String clientManagementChannelMetaAttribute(final MetaAttribute metaAttribute) {
    switch (metaAttribute) {
      case EPOCH:
        return "unix";
      case TIME_UNIT:
        return "nanosecond";
      case SEMANTIC_TYPE:
        return "";
    }

    return "";
  }

  public static int clientManagementChannelHeaderLength() {
    return 4;
  }

  public ConnectEncoder putClientManagementChannel(
      final DirectBuffer src, final int srcOffset, final int length) {
    if (length > 1073741824) {
      throw new IllegalArgumentException("length > max value for type: " + length);
    }

    final int headerLength = 4;
    final int limit = parentMessage.limit();
    parentMessage.limit(limit + headerLength + length);
    buffer.putInt(limit, length, java.nio.ByteOrder.LITTLE_ENDIAN);
    buffer.putBytes(limit + headerLength, src, srcOffset, length);

    return this;
  }

  public ConnectEncoder putClientManagementChannel(
      final byte[] src, final int srcOffset, final int length) {
    if (length > 1073741824) {
      throw new IllegalArgumentException("length > max value for type: " + length);
    }

    final int headerLength = 4;
    final int limit = parentMessage.limit();
    parentMessage.limit(limit + headerLength + length);
    buffer.putInt(limit, length, java.nio.ByteOrder.LITTLE_ENDIAN);
    buffer.putBytes(limit + headerLength, src, srcOffset, length);

    return this;
  }

  public ConnectEncoder clientManagementChannel(final String value) {
    final byte[] bytes;
    try {
      bytes = value.getBytes("UTF-8");
    } catch (final java.io.UnsupportedEncodingException ex) {
      throw new RuntimeException(ex);
    }

    final int length = bytes.length;
    if (length > 1073741824) {
      throw new IllegalArgumentException("length > max value for type: " + length);
    }

    final int headerLength = 4;
    final int limit = parentMessage.limit();
    parentMessage.limit(limit + headerLength + length);
    buffer.putInt(limit, length, java.nio.ByteOrder.LITTLE_ENDIAN);
    buffer.putBytes(limit + headerLength, bytes, 0, length);

    return this;
  }

  public String toString() {
    return appendTo(new StringBuilder(100)).toString();
  }

  public StringBuilder appendTo(final StringBuilder builder) {
    ConnectDecoder writer = new ConnectDecoder();
    writer.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

    return writer.appendTo(builder);
  }
}
