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

public class RequestNFrameFlyweight
{
    // relative to start of passed offset
    private static final int REQUEST_N_FIELD_OFFSET = FrameHeaderFlyweight.FRAME_HEADER_LENGTH;

    public static int computeFrameLength()
    {
        int length = FrameHeaderFlyweight.computeFrameHeaderLength(FrameType.REQUEST_N, 0, 0);

        return length + BitUtil.SIZE_OF_INT;
    }

    public static int encode(
        final MutableDirectBuffer mutableDirectBuffer,
        final int offset,
        final int streamId,
        final int requestN)
    {
        final int frameLength = computeFrameLength();

        int length = FrameHeaderFlyweight.encodeFrameHeader(mutableDirectBuffer, offset, frameLength, 0, FrameType.REQUEST_N, streamId);

        mutableDirectBuffer.putInt(offset + REQUEST_N_FIELD_OFFSET, requestN, ByteOrder.BIG_ENDIAN);

        return length + BitUtil.SIZE_OF_INT;
    }

    public static int requestN(final DirectBuffer directBuffer, final int offset)
    {
        return directBuffer.getInt(offset + REQUEST_N_FIELD_OFFSET, ByteOrder.BIG_ENDIAN);
    }

    public static int payloadOffset(final DirectBuffer directBuffer, final int offset)
    {
        return offset + FrameHeaderFlyweight.FRAME_HEADER_LENGTH + BitUtil.SIZE_OF_INT;
    }
}
