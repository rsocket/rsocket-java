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
  value = {"io.rsocket.aeron.internal.reactivestreams.messages.VarDataEncodingDecoder"}
)
@SuppressWarnings("all")
public class VarDataEncodingDecoder {
  public static final int ENCODED_LENGTH = -1;
  private DirectBuffer buffer;
  private int offset;

  public VarDataEncodingDecoder wrap(final DirectBuffer buffer, final int offset) {
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

  public long length() {
    return (buffer.getInt(offset + 0, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL);
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
    builder.append('(');
    // Token{signal=ENCODING, name='length', description='The channel the client listens for
    // management data on', id=-1, version=0, encodedLength=4, offset=0, componentTokenCount=1,
    // encoding=Encoding{presence=REQUIRED, primitiveType=UINT32, byteOrder=LITTLE_ENDIAN,
    // minValue=null, maxValue=1073741824, nullValue=null, constValue=null,
    // characterEncoding='UTF-8', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
    builder.append("length=");
    builder.append(length());
    builder.append('|');
    // Token{signal=ENCODING, name='varData', description='The channel the client listens for
    // management data on', id=-1, version=0, encodedLength=-1, offset=4, componentTokenCount=1,
    // encoding=Encoding{presence=REQUIRED, primitiveType=UINT8, byteOrder=LITTLE_ENDIAN,
    // minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='UTF-8',
    // epoch='unix', timeUnit=nanosecond, semanticType='null'}}
    builder.append(')');

    return builder;
  }
}
