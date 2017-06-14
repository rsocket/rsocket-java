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

public class LeaseFrameFlyweight {
  private LeaseFrameFlyweight() {}

  // relative to start of passed offset
  private static final int TTL_FIELD_OFFSET = FrameHeaderFlyweight.FRAME_HEADER_LENGTH;
  private static final int NUM_REQUESTS_FIELD_OFFSET = TTL_FIELD_OFFSET + Integer.BYTES;
  private static final int PAYLOAD_OFFSET = NUM_REQUESTS_FIELD_OFFSET + Integer.BYTES;

  public static int computeFrameLength(final int metadataLength) {
    int length = FrameHeaderFlyweight.computeFrameHeaderLength(FrameType.LEASE, metadataLength, 0);
    return length + Integer.BYTES * 2;
  }

  public static int encode(
      final ByteBuf byteBuf, final int ttl, final int numRequests, final ByteBuf metadata) {
    final int frameLength = computeFrameLength(metadata.readableBytes());

    int length =
        FrameHeaderFlyweight.encodeFrameHeader(byteBuf, frameLength, 0, FrameType.LEASE, 0);

    byteBuf.setInt(TTL_FIELD_OFFSET, ttl);
    byteBuf.setInt(NUM_REQUESTS_FIELD_OFFSET, numRequests);

    length += Integer.BYTES * 2;
    length += FrameHeaderFlyweight.encodeMetadata(byteBuf, FrameType.LEASE, length, metadata);

    return length;
  }

  public static int ttl(final ByteBuf byteBuf) {
    return byteBuf.getInt(TTL_FIELD_OFFSET);
  }

  public static int numRequests(final ByteBuf byteBuf) {
    return byteBuf.getInt(NUM_REQUESTS_FIELD_OFFSET);
  }

  public static int payloadOffset(final ByteBuf byteBuf) {
    return PAYLOAD_OFFSET;
  }
}
