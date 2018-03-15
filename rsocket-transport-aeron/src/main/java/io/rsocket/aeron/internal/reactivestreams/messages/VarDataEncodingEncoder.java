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
  value = {"io.rsocket.aeron.internal.reactivestreams.messages.VarDataEncodingEncoder"}
)
@SuppressWarnings("all")
public class VarDataEncodingEncoder {
  public static final int ENCODED_LENGTH = -1;
  private MutableDirectBuffer buffer;
  private int offset;

  public VarDataEncodingEncoder wrap(final MutableDirectBuffer buffer, final int offset) {
    this.buffer = buffer;
    this.offset = offset;

    return this;
  }

  public int encodedLength() {
    return ENCODED_LENGTH;
  }

  public static long lengthNullValue() {
    return 4294967294L;
  }

  public static long lengthMinValue() {
    return 0L;
  }

  public static long lengthMaxValue() {
    return 1073741824L;
  }

  public VarDataEncodingEncoder length(final long value) {
    buffer.putInt(offset + 0, (int) value, java.nio.ByteOrder.LITTLE_ENDIAN);
    return this;
  }

  public static short varDataNullValue() {
    return (short) 255;
  }

  public static short varDataMinValue() {
    return (short) 0;
  }

  public static short varDataMaxValue() {
    return (short) 254;
  }

  public String toString() {
    return appendTo(new StringBuilder(100)).toString();
  }

  public StringBuilder appendTo(final StringBuilder builder) {
    VarDataEncodingDecoder writer = new VarDataEncodingDecoder();
    writer.wrap(buffer, offset);

    return writer.appendTo(builder);
  }
}
