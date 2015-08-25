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
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;

import java.nio.ByteBuffer;

public class KeepaliveFrameFlyweight
{
    private static final int PAYLOAD_OFFSET = FrameHeaderFlyweight.FRAME_HEADER_LENGTH;

    public static int computeFrameLength(final int dataLength)
    {
        return FrameHeaderFlyweight.computeFrameHeaderLength(FrameType.SETUP, 0, dataLength);
    }

    public static int encode(
        final MutableDirectBuffer mutableDirectBuffer,
        final int offset,
        final ByteBuffer data)
    {
        final int frameLength = computeFrameLength(data.capacity());

        int length = FrameHeaderFlyweight.encodeFrameHeader(mutableDirectBuffer, offset, frameLength, 0, FrameType.KEEPALIVE, 0);

        length += FrameHeaderFlyweight.encodeData(mutableDirectBuffer, offset + length, data);

        return length;
    }

    public static int payloadOffset(final DirectBuffer directBuffer, final int offset)
    {
        return offset + PAYLOAD_OFFSET;
    }
}
