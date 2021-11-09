/*
 * Copyright 2015-present the original author or authors.
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
package io.rsocket.transport.aeron;

import io.netty.buffer.ByteBufUtil;
import io.rsocket.util.NumberUtils;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

final class SetupCodec {
  private SetupCodec() {}

  public static void encode(
      MutableDirectBuffer mutableDirectBuffer,
      int offset,
      long connectionId,
      int streamId,
      String channel,
      FrameType frameType) {

    PayloadCodec.encode(mutableDirectBuffer, offset, frameType);

    mutableDirectBuffer.putLong(offset + PayloadCodec.length(), connectionId);
    mutableDirectBuffer.putInt(offset + PayloadCodec.length() + Long.BYTES, streamId);

    int channelUtf8Length = NumberUtils.requireUnsignedShort(ByteBufUtil.utf8Bytes(channel));
    mutableDirectBuffer.putStringUtf8(
        offset + PayloadCodec.length() + Long.BYTES + Integer.BYTES, channel, channelUtf8Length);
  }

  public static long connectionId(DirectBuffer directBuffer, int bufferOffset) {
    int offset = bufferOffset + PayloadCodec.length();
    return directBuffer.getLong(offset);
  }

  public static int streamId(DirectBuffer directBuffer, int bufferOffset) {
    int offset = bufferOffset + PayloadCodec.length() + Long.BYTES;
    return directBuffer.getInt(offset);
  }

  public static String channel(DirectBuffer directBuffer, int bufferOffset) {
    int offset = bufferOffset + PayloadCodec.length() + Long.BYTES + Integer.BYTES;

    return directBuffer.getStringUtf8(offset);
  }

  public static int constantLength() {
    return PayloadCodec.length() + Long.BYTES + Integer.BYTES + Integer.BYTES;
  }
}
