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

public class SetupErrorFrameFlyweight
{
    // defined error codes
    public static final int INVALID_SETUP = 0x0001;
    public static final int UNSUPPORTED_SETUP = 0x0010;
    public static final int REJECTED_SETUP = 0x0100;

    // relative to start of passed offset
    private static final int ERROR_CODE_FIELD_OFFSET = FrameHeaderFlyweight.FRAME_HEADER_LENGTH;
    private static final int PAYLOAD_OFFSET = ERROR_CODE_FIELD_OFFSET + BitUtil.SIZE_OF_INT;

    public static int computeFrameLength(
        final int metadataLength,
        final int dataLength)
    {
        int length = FrameHeaderFlyweight.computeFrameHeaderLength(FrameType.SETUP, metadataLength, dataLength);

        return length + BitUtil.SIZE_OF_INT;
    }

    public static int encode(
        final MutableDirectBuffer mutableDirectBuffer,
        final int offset,
        final int errorCode,
        final ByteBuffer metadata,
        final ByteBuffer data)
    {
        final int frameLength = computeFrameLength(metadata.capacity(), data.capacity());

        int length = FrameHeaderFlyweight.encodeFrameHeader(mutableDirectBuffer, offset, frameLength, 0, FrameType.SETUP_ERROR, 0);

        mutableDirectBuffer.putInt(offset + ERROR_CODE_FIELD_OFFSET, errorCode, ByteOrder.BIG_ENDIAN);

        length += BitUtil.SIZE_OF_INT;

        length += FrameHeaderFlyweight.encodeMetadata(mutableDirectBuffer, offset, offset + length, metadata);
        length += FrameHeaderFlyweight.encodeData(mutableDirectBuffer, offset + length, data);

        return length;
    }

    public static int errorCode(final DirectBuffer directBuffer, final int offset)
    {
        return directBuffer.getInt(offset + ERROR_CODE_FIELD_OFFSET, ByteOrder.BIG_ENDIAN);
    }

    public static int payloadOffset(final DirectBuffer directBuffer, final int offset)
    {
        return offset + PAYLOAD_OFFSET;
    }
}