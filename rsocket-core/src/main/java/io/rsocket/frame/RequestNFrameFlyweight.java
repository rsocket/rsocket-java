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

public class RequestNFrameFlyweight {
  private RequestNFrameFlyweight() {}

  // relative to start of passed offset
  private static final int REQUEST_N_FIELD_OFFSET = FrameHeaderFlyweight.FRAME_HEADER_LENGTH;

  public static int computeFrameLength() {
    int length = FrameHeaderFlyweight.computeFrameHeaderLength(FrameType.REQUEST_N, 0, 0);

    return length + Integer.BYTES;
  }

  public static int encode(final ByteBuf byteBuf, final int streamId, final int requestN) {
    final int frameLength = computeFrameLength();

    int length =
        FrameHeaderFlyweight.encodeFrameHeader(
            byteBuf, frameLength, 0, FrameType.REQUEST_N, streamId);

    byteBuf.setInt(REQUEST_N_FIELD_OFFSET, requestN);

    return length + Integer.BYTES;
  }

  public static int requestN(final ByteBuf byteBuf) {
    return byteBuf.getInt(REQUEST_N_FIELD_OFFSET);
  }

  public static int payloadOffset(final ByteBuf byteBuf) {
    return FrameHeaderFlyweight.FRAME_HEADER_LENGTH + Integer.BYTES;
  }
}
