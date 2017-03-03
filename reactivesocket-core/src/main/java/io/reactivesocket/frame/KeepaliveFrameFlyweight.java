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
package io.reactivesocket.frame;

import io.reactivesocket.FrameType;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.ByteBuffer;

public class KeepaliveFrameFlyweight {
    public static final int FLAGS_KEEPALIVE_R = 0b00_1000_0000;

    private KeepaliveFrameFlyweight() {}

    private static final int LAST_POSITION_OFFSET = FrameHeaderFlyweight.FRAME_HEADER_LENGTH;
    private static final int PAYLOAD_OFFSET = LAST_POSITION_OFFSET + BitUtil.SIZE_OF_LONG;

    public static int computeFrameLength(final int dataLength) {
        return FrameHeaderFlyweight.computeFrameHeaderLength(FrameType.SETUP, 0, dataLength) + BitUtil.SIZE_OF_LONG;
    }

    public static int encode(
        final MutableDirectBuffer mutableDirectBuffer,
        final int offset,
        int flags,
        final ByteBuffer data
    ) {
        final int frameLength = computeFrameLength(data.remaining());

        int length = FrameHeaderFlyweight.encodeFrameHeader(mutableDirectBuffer, offset, frameLength, flags, FrameType.KEEPALIVE, 0);

        // We don't support resumability, last position is always zero
        mutableDirectBuffer.putLong(length, 0);
        length += BitUtil.SIZE_OF_LONG;

        length += FrameHeaderFlyweight.encodeData(mutableDirectBuffer, offset + length, data);

        return length;
    }

    public static int payloadOffset(final DirectBuffer directBuffer, final int offset) {
        return offset + PAYLOAD_OFFSET;
    }
}
