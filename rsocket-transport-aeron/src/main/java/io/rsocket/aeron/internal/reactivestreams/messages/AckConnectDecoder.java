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

@javax.annotation.Generated(
  value = {"io.rsocket.aeron.internal.reactivestreams.messages.AckConnectDecoder"}
)
@SuppressWarnings("all")
public class AckConnectDecoder {
  public static final int BLOCK_LENGTH = 12;
  public static final int TEMPLATE_ID = 2;
  public static final int SCHEMA_ID = 1;
  public static final int SCHEMA_VERSION = 0;

  private final AckConnectDecoder parentMessage = this;
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

  public AckConnectDecoder wrap(
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

  public static int serverSessionIdId() {
    return 2;
  }

  public static String serverSessionIdMetaAttribute(final MetaAttribute metaAttribute) {
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

  public static int serverSessionIdNullValue() {
    return -2147483648;
  }

  public static int serverSessionIdMinValue() {
    return -2147483647;
  }

  public static int serverSessionIdMaxValue() {
    return 2147483647;
  }

  public int serverSessionId() {
    return buffer.getInt(offset + 8, java.nio.ByteOrder.LITTLE_ENDIAN);
  }

  public String toString() {
    return appendTo(new StringBuilder(100)).toString();
  }

  public StringBuilder appendTo(final StringBuilder builder) {
    final int originalLimit = limit();
    limit(offset + actingBlockLength);
    builder.append("[AckConnect](sbeTemplateId=");
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
    // Token{signal=BEGIN_FIELD, name='serverSessionId', description='The session id for the server
    // publication', id=2, version=0, encodedLength=0, offset=8, componentTokenCount=3,
    // encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN,
    // minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null',
    // epoch='unix', timeUnit=nanosecond, semanticType='null'}}
    // Token{signal=ENCODING, name='int32', description='The session id for the server publication',
    // id=-1, version=0, encodedLength=4, offset=8, componentTokenCount=1,
    // encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN,
    // minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null',
    // epoch='unix', timeUnit=nanosecond, semanticType='null'}}
    builder.append("serverSessionId=");
    builder.append(serverSessionId());

    limit(originalLimit);

    return builder;
  }
}
