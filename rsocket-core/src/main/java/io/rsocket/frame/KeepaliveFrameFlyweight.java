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
package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.rsocket.FrameType;

public class KeepaliveFrameFlyweight {
  public static final int FLAGS_KEEPALIVE_R = 0b00_1000_0000;

  private KeepaliveFrameFlyweight() {}

  private static final int LAST_POSITION_OFFSET = FrameHeaderFlyweight.FRAME_HEADER_LENGTH;
  private static final int PAYLOAD_OFFSET = LAST_POSITION_OFFSET + Long.BYTES;

  public static int computeFrameLength(final int dataLength) {
    return FrameHeaderFlyweight.computeFrameHeaderLength(FrameType.SETUP, null, dataLength)
        + Long.BYTES;
  }

  public static int encode(final ByteBuf byteBuf, int flags, final ByteBuf data) {
    final int frameLength = computeFrameLength(data.readableBytes());

    int length =
        FrameHeaderFlyweight.encodeFrameHeader(byteBuf, frameLength, flags, FrameType.KEEPALIVE, 0);

    // We don't support resumability, last position is always zero
    byteBuf.setLong(length, 0);
    length += Long.BYTES;

    length += FrameHeaderFlyweight.encodeData(byteBuf, length, data);

    return length;
  }

  public static int payloadOffset(final ByteBuf byteBuf) {
    return PAYLOAD_OFFSET;
  }
}
