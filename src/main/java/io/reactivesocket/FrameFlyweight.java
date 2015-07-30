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
package io.reactivesocket;

import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Per connection frame flyweight.
 * Holds codecs and DirectBuffer for wrapping
 */
public class FrameFlyweight
{
    /**
     * Not the latest frame layout, but close
     * Does not include
     * - (initial) request N for REQUEST_STREAM and REQUEST_SUB and REQUEST
     * - fragmentation / reassembly
     * - encode should remove Type param and have it as part of method name (1 encode per type)
     */
    private static final boolean INCLUDE_FRAME_LENGTH = true;

    private static final int FRAME_LENGTH_FIELD_OFFSET;
    private static final int VERSION_FIELD_OFFSET;
    private static final int FLAGS_FIELD_OFFSET;
    private static final int TYPE_FIELD_OFFSET;
    private static final int STREAM_ID_FIELD_OFFSET;
    private static final int DATA_OFFSET;

    private static final byte CURRENT_VERSION = 0;

    private static final int FLAGS_I = 0b1000_000;
    private static final int FLAGS_B = 0b0100_000;
    private static final int FLAGS_E = 0b0010_000;
    private static final int FLAGS_C = 0b0001_000;

    static
    {
        if (INCLUDE_FRAME_LENGTH)
        {
            FRAME_LENGTH_FIELD_OFFSET = 0;
        }
        else
        {
            FRAME_LENGTH_FIELD_OFFSET = - BitUtil.SIZE_OF_INT;
        }

        VERSION_FIELD_OFFSET = FRAME_LENGTH_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
        FLAGS_FIELD_OFFSET = VERSION_FIELD_OFFSET + BitUtil.SIZE_OF_BYTE;
        TYPE_FIELD_OFFSET = FLAGS_FIELD_OFFSET + BitUtil.SIZE_OF_BYTE;
        STREAM_ID_FIELD_OFFSET = TYPE_FIELD_OFFSET + BitUtil.SIZE_OF_SHORT;
        DATA_OFFSET = STREAM_ID_FIELD_OFFSET + BitUtil.SIZE_OF_LONG;
    }

    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    // single threaded assumed
    private final MutableDirectBuffer frameBuffer = new UnsafeBuffer(EMPTY_BUFFER);

    public static int computeFrameLength(final int dataLength)
    {
        return DATA_OFFSET + dataLength;
    }

    public int encode(final ByteBuffer byteBuffer, final long streamId, final FrameType type, final byte[] data)
    {
        final int frameLength = computeFrameLength(data.length);

        frameBuffer.wrap(byteBuffer);

        if (INCLUDE_FRAME_LENGTH)
        {
            frameBuffer.putInt(FRAME_LENGTH_FIELD_OFFSET, frameLength, ByteOrder.BIG_ENDIAN);
        }

        final FrameType outFrameType;
        int flags = 0;

        switch (type)
        {
            case COMPLETE:
                outFrameType = FrameType.RESPONSE;
                flags |= FLAGS_C;
                break;
            case NEXT:
                outFrameType = FrameType.RESPONSE;
                break;
            default:
                outFrameType = type;
                break;
        }

        frameBuffer.putByte(VERSION_FIELD_OFFSET, CURRENT_VERSION);
        frameBuffer.putByte(FLAGS_FIELD_OFFSET, (byte) flags);
        frameBuffer.putShort(TYPE_FIELD_OFFSET, (short)outFrameType.getEncodedType(), ByteOrder.BIG_ENDIAN);
        frameBuffer.putLong(STREAM_ID_FIELD_OFFSET, streamId, ByteOrder.BIG_ENDIAN);
        frameBuffer.putBytes(DATA_OFFSET, data);

        return frameLength;
    }

    public void decode(Frame frame, final ByteBuffer byteBuffer, final int length)
    {
        frameBuffer.wrap(byteBuffer);

        int frameLength = length;

        if (INCLUDE_FRAME_LENGTH)
        {
            frameLength = frameBuffer.getInt(FRAME_LENGTH_FIELD_OFFSET, ByteOrder.BIG_ENDIAN);
        }

        final int version = frameBuffer.getByte(VERSION_FIELD_OFFSET);
        final int flags = frameBuffer.getByte(FLAGS_FIELD_OFFSET);
        FrameType frameType = FrameType.from(frameBuffer.getShort(TYPE_FIELD_OFFSET, ByteOrder.BIG_ENDIAN));

        final long streamId = frameBuffer.getLong(STREAM_ID_FIELD_OFFSET, ByteOrder.BIG_ENDIAN);

        final int dataLength = frameLength - DATA_OFFSET;
        int dataOffset = DATA_OFFSET;

        switch (frameType)
        {
            case RESPONSE:
                if (FLAGS_C == (flags & FLAGS_C))
                {
                    frameType = FrameType.COMPLETE;
                }
                else
                {
                    frameType = FrameType.NEXT;
                }
                break;

            case REQUEST_N:
                // TODO: grab N value
                break;
            case REQUEST_STREAM:
                // TODO: grab N value, and move DATA_OFFSET value
                break;
            case REQUEST_SUBSCRIPTION:
                // TODO: grab N value, and move DATA_OFFSET value
                break;
        }

        // fill in Frame fields
        frame.setFromDecode(version, streamId, frameType, flags);
        frame.setFromDecode(frameBuffer, dataOffset, dataLength);
    }
}
