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
  value = {"io.rsocket.aeron.internal.reactivestreams.messages.ConnectDecoder"}
)
@SuppressWarnings("all")
public class ConnectDecoder {
  public static final int BLOCK_LENGTH = 20;
  public static final int TEMPLATE_ID = 1;
  public static final int SCHEMA_ID = 1;
  public static final int SCHEMA_VERSION = 0;

  private final ConnectDecoder parentMessage = this;
  private DirectBuffer buffer;
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

  public ConnectDecoder wrap(
      final DirectBuffer buffer,
      final int offset,
      final int actingBlockLength,
      final int actingVersion) {
    this.buffer = buffer;
    this.offset = offset;
    this.actingBlockLength = actingBlockLength;
    this.actingVersion = actingVersion;
    limit(offset + actingBlockLength);

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

  public static int channelIdId() {
    return 1;
  }

  public static String channelIdMetaAttribute(final MetaAttribute metaAttribute) {
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

  public static long channelIdNullValue() {
    return -9223372036854775808L;
  }

  public static long channelIdMinValue() {
    return -9223372036854775807L;
  }

  public static long channelIdMaxValue() {
    return 9223372036854775807L;
  }

  public long channelId() {
    return buffer.getLong(offset + 0, java.nio.ByteOrder.LITTLE_ENDIAN);
  }

  public static int sendingStreamIdId() {
    return 2;
  }

  public static String sendingStreamIdMetaAttribute(final MetaAttribute metaAttribute) {
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

  public static int sendingStreamIdNullValue() {
    return -2147483648;
  }

  public static int sendingStreamIdMinValue() {
    return -2147483647;
  }

  public static int sendingStreamIdMaxValue() {
    return 2147483647;
  }

  public int sendingStreamId() {
    return buffer.getInt(offset + 8, java.nio.ByteOrder.LITTLE_ENDIAN);
  }

  public static int receivingStreamIdId() {
    return 3;
  }

  public static String receivingStreamIdMetaAttribute(final MetaAttribute metaAttribute) {
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

  public static int receivingStreamIdNullValue() {
    return -2147483648;
  }

  public static int receivingStreamIdMinValue() {
    return -2147483647;
  }

  public static int receivingStreamIdMaxValue() {
    return 2147483647;
  }

  public int receivingStreamId() {
    return buffer.getInt(offset + 12, java.nio.ByteOrder.LITTLE_ENDIAN);
  }

  public static int clientSessionIdId() {
    return 4;
  }

  public static String clientSessionIdMetaAttribute(final MetaAttribute metaAttribute) {
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

  public static int clientSessionIdNullValue() {
    return -2147483648;
  }

  public static int clientSessionIdMinValue() {
    return -2147483647;
  }

  public static int clientSessionIdMaxValue() {
    return 2147483647;
  }

  public int clientSessionId() {
    return buffer.getInt(offset + 16, java.nio.ByteOrder.LITTLE_ENDIAN);
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

  public int sendingChannelLength() {
    final int limit = parentMessage.limit();
    return (int) (buffer.getInt(limit, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL);
  }

  public int getSendingChannel(
      final MutableDirectBuffer dst, final int dstOffset, final int length) {
    final int headerLength = 4;
    final int limit = parentMessage.limit();
    final int dataLength =
        (int) (buffer.getInt(limit, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL);
    final int bytesCopied = Math.min(length, dataLength);
    parentMessage.limit(limit + headerLength + dataLength);
    buffer.getBytes(limit + headerLength, dst, dstOffset, bytesCopied);

    return bytesCopied;
  }

  public int getSendingChannel(final byte[] dst, final int dstOffset, final int length) {
    final int headerLength = 4;
    final int limit = parentMessage.limit();
    final int dataLength =
        (int) (buffer.getInt(limit, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL);
    final int bytesCopied = Math.min(length, dataLength);
    parentMessage.limit(limit + headerLength + dataLength);
    buffer.getBytes(limit + headerLength, dst, dstOffset, bytesCopied);

    return bytesCopied;
  }

  public String sendingChannel() {
    final int headerLength = 4;
    final int limit = parentMessage.limit();
    final int dataLength =
        (int) (buffer.getInt(limit, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL);
    parentMessage.limit(limit + headerLength + dataLength);
    final byte[] tmp = new byte[dataLength];
    buffer.getBytes(limit + headerLength, tmp, 0, dataLength);

    final String value;
    try {
      value = new String(tmp, "UTF-8");
    } catch (final java.io.UnsupportedEncodingException ex) {
      throw new RuntimeException(ex);
    }

    return value;
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

  public int receivingChannelLength() {
    final int limit = parentMessage.limit();
    return (int) (buffer.getInt(limit, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL);
  }

  public int getReceivingChannel(
      final MutableDirectBuffer dst, final int dstOffset, final int length) {
    final int headerLength = 4;
    final int limit = parentMessage.limit();
    final int dataLength =
        (int) (buffer.getInt(limit, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL);
    final int bytesCopied = Math.min(length, dataLength);
    parentMessage.limit(limit + headerLength + dataLength);
    buffer.getBytes(limit + headerLength, dst, dstOffset, bytesCopied);

    return bytesCopied;
  }

  public int getReceivingChannel(final byte[] dst, final int dstOffset, final int length) {
    final int headerLength = 4;
    final int limit = parentMessage.limit();
    final int dataLength =
        (int) (buffer.getInt(limit, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL);
    final int bytesCopied = Math.min(length, dataLength);
    parentMessage.limit(limit + headerLength + dataLength);
    buffer.getBytes(limit + headerLength, dst, dstOffset, bytesCopied);

    return bytesCopied;
  }

  public String receivingChannel() {
    final int headerLength = 4;
    final int limit = parentMessage.limit();
    final int dataLength =
        (int) (buffer.getInt(limit, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL);
    parentMessage.limit(limit + headerLength + dataLength);
    final byte[] tmp = new byte[dataLength];
    buffer.getBytes(limit + headerLength, tmp, 0, dataLength);

    final String value;
    try {
      value = new String(tmp, "UTF-8");
    } catch (final java.io.UnsupportedEncodingException ex) {
      throw new RuntimeException(ex);
    }

    return value;
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

  public int clientManagementChannelLength() {
    final int limit = parentMessage.limit();
    return (int) (buffer.getInt(limit, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL);
  }

  public int getClientManagementChannel(
      final MutableDirectBuffer dst, final int dstOffset, final int length) {
    final int headerLength = 4;
    final int limit = parentMessage.limit();
    final int dataLength =
        (int) (buffer.getInt(limit, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL);
    final int bytesCopied = Math.min(length, dataLength);
    parentMessage.limit(limit + headerLength + dataLength);
    buffer.getBytes(limit + headerLength, dst, dstOffset, bytesCopied);

    return bytesCopied;
  }

  public int getClientManagementChannel(final byte[] dst, final int dstOffset, final int length) {
    final int headerLength = 4;
    final int limit = parentMessage.limit();
    final int dataLength =
        (int) (buffer.getInt(limit, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL);
    final int bytesCopied = Math.min(length, dataLength);
    parentMessage.limit(limit + headerLength + dataLength);
    buffer.getBytes(limit + headerLength, dst, dstOffset, bytesCopied);

    return bytesCopied;
  }

  public String clientManagementChannel() {
    final int headerLength = 4;
    final int limit = parentMessage.limit();
    final int dataLength =
        (int) (buffer.getInt(limit, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL);
    parentMessage.limit(limit + headerLength + dataLength);
    final byte[] tmp = new byte[dataLength];
    buffer.getBytes(limit + headerLength, tmp, 0, dataLength);

    final String value;
    try {
      value = new String(tmp, "UTF-8");
    } catch (final java.io.UnsupportedEncodingException ex) {
      throw new RuntimeException(ex);
    }

    return value;
  }

  public String toString() {
    return appendTo(new StringBuilder(100)).toString();
  }

  public StringBuilder appendTo(final StringBuilder builder) {
    final int originalLimit = limit();
    limit(offset + actingBlockLength);
    builder.append("[Connect](sbeTemplateId=");
    builder.append(TEMPLATE_ID);
    builder.append("|sbeSchemaId=");
    builder.append(SCHEMA_ID);
    builder.append("|sbeSchemaVersion=");
    if (actingVersion != SCHEMA_VERSION) {
      builder.append(actingVersion);
      builder.append('/');
    }
    builder.append(SCHEMA_VERSION);
    builder.append("|sbeBlockLength=");
    if (actingBlockLength != BLOCK_LENGTH) {
      builder.append(actingBlockLength);
      builder.append('/');
    }
    builder.append(BLOCK_LENGTH);
    builder.append("):");
    // Token{signal=BEGIN_FIELD, name='channelId', description='The AeronChannel id', id=1,
    // version=0, encodedLength=0, offset=0, componentTokenCount=3,
    // encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN,
    // minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null',
    // epoch='unix', timeUnit=nanosecond, semanticType='null'}}
    // Token{signal=ENCODING, name='int64', description='The AeronChannel id', id=-1, version=0,
    // encodedLength=8, offset=0, componentTokenCount=1, encoding=Encoding{presence=REQUIRED,
    // primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null,
    // constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond,
    // semanticType='null'}}
    builder.append("channelId=");
    builder.append(channelId());
    builder.append('|');
    // Token{signal=BEGIN_FIELD, name='sendingStreamId', description='The stream id the connecting
    // client will send traffic on', id=2, version=0, encodedLength=0, offset=8,
    // componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null,
    // byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null,
    // characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
    // Token{signal=ENCODING, name='int32', description='The stream id the connecting client will
    // send traffic on', id=-1, version=0, encodedLength=4, offset=8, componentTokenCount=1,
    // encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN,
    // minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null',
    // epoch='unix', timeUnit=nanosecond, semanticType='null'}}
    builder.append("sendingStreamId=");
    builder.append(sendingStreamId());
    builder.append('|');
    // Token{signal=BEGIN_FIELD, name='receivingStreamId', description='The stream id the connecting
    // client will receive data on', id=3, version=0, encodedLength=0, offset=12,
    // componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null,
    // byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null,
    // characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
    // Token{signal=ENCODING, name='int32', description='The stream id the connecting client will
    // receive data on', id=-1, version=0, encodedLength=4, offset=12, componentTokenCount=1,
    // encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN,
    // minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null',
    // epoch='unix', timeUnit=nanosecond, semanticType='null'}}
    builder.append("receivingStreamId=");
    builder.append(receivingStreamId());
    builder.append('|');
    // Token{signal=BEGIN_FIELD, name='clientSessionId', description='The session id for the client
    // publication', id=4, version=0, encodedLength=0, offset=16, componentTokenCount=3,
    // encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN,
    // minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null',
    // epoch='unix', timeUnit=nanosecond, semanticType='null'}}
    // Token{signal=ENCODING, name='int32', description='The session id for the client publication',
    // id=-1, version=0, encodedLength=4, offset=16, componentTokenCount=1,
    // encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN,
    // minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null',
    // epoch='unix', timeUnit=nanosecond, semanticType='null'}}
    builder.append("clientSessionId=");
    builder.append(clientSessionId());
    builder.append('|');
    // Token{signal=BEGIN_VAR_DATA, name='sendingChannel', description='The Aeron channel the client
    // will send data on', id=5, version=0, encodedLength=0, offset=20, componentTokenCount=6,
    // encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN,
    // minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null',
    // epoch='unix', timeUnit=nanosecond, semanticType='null'}}
    builder.append("sendingChannel=");
    builder.append(sendingChannel());
    builder.append('|');
    // Token{signal=BEGIN_VAR_DATA, name='receivingChannel', description='The Aeron channel the
    // client will receive data on', id=6, version=0, encodedLength=0, offset=-1,
    // componentTokenCount=6, encoding=Encoding{presence=REQUIRED, primitiveType=null,
    // byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null,
    // characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
    builder.append("receivingChannel=");
    builder.append(receivingChannel());
    builder.append('|');
    // Token{signal=BEGIN_VAR_DATA, name='clientManagementChannel', description='The channel the
    // client listens for management data on', id=6, version=0, encodedLength=0, offset=-1,
    // componentTokenCount=6, encoding=Encoding{presence=REQUIRED, primitiveType=null,
    // byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null,
    // characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
    builder.append("clientManagementChannel=");
    builder.append(clientManagementChannel());

    limit(originalLimit);

    return builder;
  }
}
