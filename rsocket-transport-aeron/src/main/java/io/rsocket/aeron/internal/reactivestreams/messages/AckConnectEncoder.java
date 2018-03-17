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

import org.agrona.MutableDirectBuffer;

@javax.annotation.Generated(
  value = {"io.rsocket.aeron.internal.reactivestreams.messages.AckConnectEncoder"}
)
@SuppressWarnings("all")
public class AckConnectEncoder {
  public static final int BLOCK_LENGTH = 12;
  public static final int TEMPLATE_ID = 2;
  public static final int SCHEMA_ID = 1;
  public static final int SCHEMA_VERSION = 0;

  private final AckConnectEncoder parentMessage = this;
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

  public AckConnectEncoder wrap(final MutableDirectBuffer buffer, final int offset) {
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

  public AckConnectEncoder channelId(final long value) {
    buffer.putLong(offset + 0, value, java.nio.ByteOrder.LITTLE_ENDIAN);
    return this;
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

  public AckConnectEncoder serverSessionId(final int value) {
    buffer.putInt(offset + 8, value, java.nio.ByteOrder.LITTLE_ENDIAN);
    return this;
  }

  public String toString() {
    return appendTo(new StringBuilder(100)).toString();
  }

  public StringBuilder appendTo(final StringBuilder builder) {
    AckConnectDecoder writer = new AckConnectDecoder();
    writer.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

    return writer.appendTo(builder);
  }
}
