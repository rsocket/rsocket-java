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
import javax.annotation.Nullable;

public class RequestFrameFlyweight {

  private RequestFrameFlyweight() {}

  // relative to start of passed offset
  private static final int INITIAL_REQUEST_N_FIELD_OFFSET =
      FrameHeaderFlyweight.FRAME_HEADER_LENGTH;

  public static int computeFrameLength(
      final FrameType type, final @Nullable Integer metadataLength, final int dataLength) {
    int length = FrameHeaderFlyweight.computeFrameHeaderLength(type, metadataLength, dataLength);

    if (type.hasInitialRequestN()) {
      length += Integer.BYTES;
    }

    return length;
  }

  public static int encode(
      final ByteBuf byteBuf,
      final int streamId,
      int flags,
      final FrameType type,
      final int initialRequestN,
      final @Nullable ByteBuf metadata,
      final ByteBuf data) {
    final int frameLength =
        computeFrameLength(type, metadata != null ? metadata.readableBytes() : null,
            data.readableBytes());

    int length =
        FrameHeaderFlyweight.encodeFrameHeader(byteBuf, frameLength, flags, type, streamId);

    byteBuf.setInt(INITIAL_REQUEST_N_FIELD_OFFSET, initialRequestN);
    length += Integer.BYTES;

    length += FrameHeaderFlyweight.encodeMetadata(byteBuf, type, length, metadata);
    length += FrameHeaderFlyweight.encodeData(byteBuf, length, data);

    return length;
  }

  public static int encode(
      final ByteBuf byteBuf,
      final int streamId,
      final int flags,
      final FrameType type,
      final @Nullable ByteBuf metadata,
      final ByteBuf data) {
    if (type.hasInitialRequestN()) {
      throw new AssertionError(type + " must not be encoded without initial request N");
    }
    final int frameLength =
        computeFrameLength(type, metadata != null ? metadata.readableBytes() : null,
            data.readableBytes());

    int length =
        FrameHeaderFlyweight.encodeFrameHeader(byteBuf, frameLength, flags, type, streamId);

    length += FrameHeaderFlyweight.encodeMetadata(byteBuf, type, length, metadata);
    length += FrameHeaderFlyweight.encodeData(byteBuf, length, data);

    return length;
  }

  public static int initialRequestN(final ByteBuf byteBuf) {
    return byteBuf.getInt(INITIAL_REQUEST_N_FIELD_OFFSET);
  }

  public static int payloadOffset(final FrameType type, final ByteBuf byteBuf) {
    int result = FrameHeaderFlyweight.FRAME_HEADER_LENGTH;

    if (type.hasInitialRequestN()) {
      result += Integer.BYTES;
    }

    return result;
  }
}
