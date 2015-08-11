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
 *
 * Not the latest frame layout, but close.
 * Does not include
 * - (initial) request N for REQUEST_STREAM and REQUEST_SUB and REQUEST
 * - fragmentation / reassembly
 * - encode should remove Type param and have it as part of method name (1 encode per type)
 *
 * TODO: instead of ByteBuffer, methods should take MutableDirectBuffer (pre-wrapped)
 *
 * Not thread-safe. Assumed to be used single-threaded
 */
public class FrameFlyweight
{
    public static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
    public static final byte[] NULL_BYTE_ARRAY = new byte[0];

    private static final boolean INCLUDE_FRAME_LENGTH = true;

    private static final int INITIAL_MESSAGE_ARRAY_SIZE = 16;

    private static final int FRAME_LENGTH_FIELD_OFFSET;
    private static final int VERSION_FIELD_OFFSET;
    private static final int FLAGS_FIELD_OFFSET;
    private static final int TYPE_FIELD_OFFSET;
    private static final int STREAM_ID_FIELD_OFFSET;
    private static final int PAYLOAD_OFFSET;

    private static final byte CURRENT_VERSION = 0;

    private static final int FLAGS_I = 0b1000_000;
    private static final int FLAGS_M = 0b0100_000;
    private static final int FLAGS_RESPONSE_F = 0b0010_000;
    private static final int FLAGS_RESPONSE_C = 0b0001_000;

    static
    {
        if (INCLUDE_FRAME_LENGTH)
        {
            FRAME_LENGTH_FIELD_OFFSET = 0;
        }
        else
        {
            FRAME_LENGTH_FIELD_OFFSET = -BitUtil.SIZE_OF_INT;
        }

        VERSION_FIELD_OFFSET = FRAME_LENGTH_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
        FLAGS_FIELD_OFFSET = VERSION_FIELD_OFFSET + BitUtil.SIZE_OF_BYTE;
        TYPE_FIELD_OFFSET = FLAGS_FIELD_OFFSET + BitUtil.SIZE_OF_BYTE;
        STREAM_ID_FIELD_OFFSET = TYPE_FIELD_OFFSET + BitUtil.SIZE_OF_SHORT;
        PAYLOAD_OFFSET = STREAM_ID_FIELD_OFFSET + BitUtil.SIZE_OF_LONG;
    }

    private final MutableDirectBuffer frameBuffer = new UnsafeBuffer(EMPTY_BUFFER);
    private byte[] byteArray = new byte[INITIAL_MESSAGE_ARRAY_SIZE];

    public static int computeFrameLength(final FrameType frameType, int metadataLength, final int dataLength)
    {
        return payloadOffset(frameType) + metadataLength(metadataLength) + dataLength;
    }

    public int encode(
        final ByteBuffer byteBuffer,
        final long streamId,
        final FrameType type,
        final String metadata,
        final String data)
    {
        final byte[] metadataBytes = (null != metadata) ? metadata.getBytes() : NULL_BYTE_ARRAY;
        final byte[] dataBytes = (null != data) ? data.getBytes() : NULL_BYTE_ARRAY;

        return encode(byteBuffer, streamId, type, metadataBytes, dataBytes);
    }

    public int encode(
        final ByteBuffer byteBuffer,
        final long streamId,
        final FrameType frameType,
        final byte[] metadata,
        final byte[] data)
    {
        final int frameLength = computeFrameLength(frameType, metadata.length, data.length);

        frameBuffer.wrap(byteBuffer);

        if (INCLUDE_FRAME_LENGTH)
        {
            frameBuffer.putInt(FRAME_LENGTH_FIELD_OFFSET, frameLength, ByteOrder.BIG_ENDIAN);
        }

        final FrameType outFrameType;
        int flags = 0;
        int offset = payloadOffset(frameType);
        int metadataLength = metadataLength(metadata.length);

        switch (frameType)
        {
            case COMPLETE:
                outFrameType = FrameType.RESPONSE;
                flags |= FLAGS_RESPONSE_C;
                break;
            case NEXT:
                outFrameType = FrameType.RESPONSE;
                break;
            default:
                outFrameType = frameType;
                break;
        }

        if (0 < metadataLength)
        {
            flags |= FLAGS_M;
        }

        frameBuffer.putByte(VERSION_FIELD_OFFSET, CURRENT_VERSION);
        frameBuffer.putByte(FLAGS_FIELD_OFFSET, (byte) flags);
        frameBuffer.putShort(TYPE_FIELD_OFFSET, (short) outFrameType.getEncodedType(), ByteOrder.BIG_ENDIAN);
        frameBuffer.putLong(STREAM_ID_FIELD_OFFSET, streamId, ByteOrder.BIG_ENDIAN);
        frameBuffer.putBytes(offset, metadata);
        offset += metadataLength;
        frameBuffer.putBytes(offset, data);

        return frameLength;
    }

    public int version(final ByteBuffer byteBuffer)
    {
        frameBuffer.wrap(byteBuffer);
        return frameBuffer.getByte(VERSION_FIELD_OFFSET);
    }

    public int flags(final ByteBuffer byteBuffer)
    {
        frameBuffer.wrap(byteBuffer);
        return frameBuffer.getByte(FLAGS_FIELD_OFFSET);
    }

    public FrameType frameType(final ByteBuffer byteBuffer)
    {
        frameBuffer.wrap(byteBuffer);
        FrameType result = FrameType.from(frameBuffer.getShort(TYPE_FIELD_OFFSET, ByteOrder.BIG_ENDIAN));
        final int dataLength = dataLength(byteBuffer, 0);

        if (FrameType.RESPONSE == result)
        {
            final int flags = flags(byteBuffer);

            if (FLAGS_RESPONSE_C == (flags & FLAGS_RESPONSE_C) && 0 < dataLength)
            {
                result = FrameType.NEXT_COMPLETE;
            }
            else if (FLAGS_RESPONSE_C == (flags & FLAGS_RESPONSE_C))
            {
                result = FrameType.COMPLETE;
            }
            else
            {
                result = FrameType.NEXT;
            }
        }

        return result;
    }

    public long streamId(final ByteBuffer byteBuffer)
    {
        frameBuffer.wrap(byteBuffer);
        return frameBuffer.getLong(STREAM_ID_FIELD_OFFSET, ByteOrder.BIG_ENDIAN);
    }

    public String frameData(final ByteBuffer byteBuffer, final int length)
    {
        frameBuffer.wrap(byteBuffer);

        final int dataLength = dataLength(byteBuffer, length);
        final int dataOffset = dataOffset(byteBuffer);

        // byteArray used as a re-usable temporary for generating payload String
        ensureByteArrayCapacity(dataLength);
        frameBuffer.getBytes(dataOffset, byteArray, 0, dataLength);

        return new String(byteArray, 0, dataLength);
    }

    public ByteBuffer sliceFrameData(final ByteBuffer byteBuffer, final int length)
    {
        final int dataLength = dataLength(byteBuffer, length);
        final int dataOffset = dataOffset(byteBuffer);

        return slice(byteBuffer, dataOffset, dataOffset + dataLength);
    }

    public ByteBuffer sliceFrameMetadata(final ByteBuffer byteBuffer, final int length)
    {
        final int metadataLength = metadataLength(byteBuffer);
        final int metadataOffset = metadataOffset(byteBuffer);

        return slice(byteBuffer, metadataOffset, metadataOffset + metadataLength);
    }

    // really should be an interface to ByteBuffer... sigh
    // TODO: move to some utility package
    public static ByteBuffer slice(final ByteBuffer byteBuffer, final int position, final int limit)
    {
        final int savedPosition = byteBuffer.position();
        final int savedLimit = byteBuffer.limit();

        byteBuffer.limit(limit).position(position);

        final ByteBuffer result = byteBuffer.slice();

        byteBuffer.limit(savedLimit).position(savedPosition);
        return result;
    }

    private void ensureByteArrayCapacity(final int length)
    {
        if (byteArray.length < length)
        {
            byteArray = new byte[length];
        }
    }

    private int frameLength(final ByteBuffer byteBuffer, final int externalFrameLength)
    {
        frameBuffer.wrap(byteBuffer);

        int frameLength = externalFrameLength;

        if (INCLUDE_FRAME_LENGTH)
        {
            frameLength = frameBuffer.getInt(FRAME_LENGTH_FIELD_OFFSET, ByteOrder.BIG_ENDIAN);
        }

        return frameLength;
    }

    private static int metadataLength(final int metadataPayloadLength)
    {
        return metadataPayloadLength + ((0 == metadataPayloadLength) ? 0 : BitUtil.SIZE_OF_INT);
    }

    private int metadataLength(final ByteBuffer byteBuffer)
    {
        frameBuffer.wrap(byteBuffer);

        int metadataLength = 0;

        if (FLAGS_M == (FLAGS_M & frameBuffer.getByte(FLAGS_FIELD_OFFSET)))
        {
            metadataLength = (frameBuffer.getInt(metadataOffset(byteBuffer)) & 0xFFFFFF) + BitUtil.SIZE_OF_INT;
        }

        return metadataLength;
    }

    private int dataLength(final ByteBuffer byteBuffer, final int length)
    {
        final int frameLength = frameLength(byteBuffer, length);
        final int metadataLength = metadataLength(byteBuffer);

        return frameLength - metadataLength - payloadOffset(byteBuffer);
    }

    private static int payloadOffset(final FrameType frameType)
    {
        int result = PAYLOAD_OFFSET;

        if (FrameType.REQUEST_STREAM == frameType || FrameType.REQUEST_SUBSCRIPTION == frameType)
        {
            result += BitUtil.SIZE_OF_LONG;
        }

        return result;
    }

    private int payloadOffset(final ByteBuffer byteBuffer)
    {
        frameBuffer.wrap(byteBuffer);
        final FrameType frameType = FrameType.from(frameBuffer.getShort(TYPE_FIELD_OFFSET, ByteOrder.BIG_ENDIAN));

        return payloadOffset(frameType);
    }

    private int metadataOffset(final ByteBuffer byteBuffer)
    {
        return payloadOffset(byteBuffer);
    }

    private int dataOffset(final ByteBuffer byteBuffer)
    {
        return payloadOffset(byteBuffer) + metadataLength(byteBuffer);
    }
}
