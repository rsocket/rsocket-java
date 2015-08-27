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

public class RequestFrameFlyweight
{
    // relative to start of passed offset
    private static final int INITIAL_REQUEST_N_FIELD_OFFSET = FrameHeaderFlyweight.FRAME_HEADER_LENGTH;

    public static int computeFrameLength(final FrameType type, final int metadataLength, final int dataLength)
    {
        int length = FrameHeaderFlyweight.computeFrameHeaderLength(type, metadataLength, dataLength);

        if (type.hasInitialRequestN())
        {
            length += BitUtil.SIZE_OF_LONG;
        }

        return length;
    }

    public static int encode(
        final MutableDirectBuffer mutableDirectBuffer,
        final int offset,
        final long streamId,
        final FrameType type,
        final long initialRequestN,
        final ByteBuffer metadata,
        final ByteBuffer data)
    {
        final int frameLength = computeFrameLength(type, metadata.capacity(), data.capacity());

        int length = FrameHeaderFlyweight.encodeFrameHeader(mutableDirectBuffer, offset, frameLength, 0, type, streamId);

        mutableDirectBuffer.putLong(offset + INITIAL_REQUEST_N_FIELD_OFFSET, initialRequestN, ByteOrder.BIG_ENDIAN);
        length += BitUtil.SIZE_OF_LONG;

        length += FrameHeaderFlyweight.encodeMetadata(mutableDirectBuffer, offset, offset + length, metadata);
        length += FrameHeaderFlyweight.encodeData(mutableDirectBuffer, offset + length, data);

        return length;
    }

    public static int encode(
        final MutableDirectBuffer mutableDirectBuffer,
        final int offset,
        final long streamId,
        final FrameType type,
        final ByteBuffer metadata,
        final ByteBuffer data)
    {
        final int frameLength = computeFrameLength(type, metadata.capacity(), data.capacity());

        int length = FrameHeaderFlyweight.encodeFrameHeader(mutableDirectBuffer, offset, frameLength, 0, type, streamId);

        length += FrameHeaderFlyweight.encodeMetadata(mutableDirectBuffer, offset, offset + length, metadata);
        length += FrameHeaderFlyweight.encodeData(mutableDirectBuffer, offset + length, data);

        return length;
    }

    public static long initialRequestN(final DirectBuffer directBuffer, final int offset)
    {
        return directBuffer.getLong(offset + INITIAL_REQUEST_N_FIELD_OFFSET, ByteOrder.BIG_ENDIAN);
    }

    public static int payloadOffset(final FrameType type, final DirectBuffer directBuffer, final int offset)
    {
        int result = offset + FrameHeaderFlyweight.FRAME_HEADER_LENGTH;

        if (type.hasInitialRequestN())
        {
            result += BitUtil.SIZE_OF_LONG;
        }

        return result;
    }
}
