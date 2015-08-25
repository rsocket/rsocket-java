/**
 * Copyright 2015 Netflix, Inc.
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
package io.reactivesocket.internal;

import io.reactivesocket.FrameType;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class LeaseFrameFlyweight
{
    // relative to start of passed offset
    private static final int TTL_FIELD_OFFSET = FrameHeaderFlyweight.FRAME_HEADER_LENGTH;
    private static final int NUM_REQUESTS_FIELD_OFFSET = TTL_FIELD_OFFSET + BitUtil.SIZE_OF_LONG;
    private static final int PAYLOAD_OFFSET = NUM_REQUESTS_FIELD_OFFSET + BitUtil.SIZE_OF_LONG;

    public static int computeFrameLength(final int metadataLength)
    {
        int length = FrameHeaderFlyweight.computeFrameHeaderLength(FrameType.SETUP, metadataLength, 0);

        return length + BitUtil.SIZE_OF_LONG * 2;
    }

    public static int encode(
        final MutableDirectBuffer mutableDirectBuffer,
        final int offset,
        final long ttl,
        final long numRequests,
        final ByteBuffer metadata)
    {
        final int frameLength = computeFrameLength(metadata.capacity());

        int length = FrameHeaderFlyweight.encodeFrameHeader(mutableDirectBuffer, offset, frameLength, 0, FrameType.LEASE, 0);

        mutableDirectBuffer.putLong(offset + TTL_FIELD_OFFSET, ttl, ByteOrder.BIG_ENDIAN);
        mutableDirectBuffer.putLong(offset + NUM_REQUESTS_FIELD_OFFSET, numRequests, ByteOrder.BIG_ENDIAN);

        length += BitUtil.SIZE_OF_LONG * 2;
        length += FrameHeaderFlyweight.encodeMetadata(mutableDirectBuffer, offset, offset + length, metadata);

        return length;
    }

    public static int ttl(final DirectBuffer directBuffer, final int offset)
    {
        return directBuffer.getInt(offset + TTL_FIELD_OFFSET, ByteOrder.BIG_ENDIAN);
    }

    public static int numRequests(final DirectBuffer directBuffer, final int offset)
    {
        return directBuffer.getInt(offset + NUM_REQUESTS_FIELD_OFFSET, ByteOrder.BIG_ENDIAN);
    }

    public static int payloadOffset(final DirectBuffer directBuffer, final int offset)
    {
        return offset + PAYLOAD_OFFSET;
    }
}
