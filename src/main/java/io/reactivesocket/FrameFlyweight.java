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
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Per connection frame flyweight.
 *
 * Not the latest frame layout, but close.
 * Does not include
 * - set initial request N for REQUEST_STREAM and REQUEST_SUB and REQUEST
 * - fragmentation / reassembly
 * - encode should remove Type param and have it as part of method name (1 encode per type?)
 *
 * Not thread-safe. Assumed to be used single-threaded
 */
public class FrameFlyweight
{
    public static final byte[] NULL_BYTE_ARRAY = new byte[0];

    private static final boolean INCLUDE_FRAME_LENGTH = true;

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

    public static int computeFrameLength(final FrameType frameType, int metadataLength, final int dataLength)
    {
        return payloadOffset(frameType) + metadataLength(metadataLength) + dataLength;
    }

    public static int encode(
        final MutableDirectBuffer mutableDirectBuffer,
        final long streamId,
        final FrameType type,
        final String metadata,
        final String data)
    {
        final byte[] metadataBytes = (null != metadata) ? metadata.getBytes() : NULL_BYTE_ARRAY;
        final byte[] dataBytes = (null != data) ? data.getBytes() : NULL_BYTE_ARRAY;

        return encode(mutableDirectBuffer, streamId, type, metadataBytes, dataBytes);
    }

    public static int encode(
        final MutableDirectBuffer mutableDirectBuffer,
        final long streamId,
        final FrameType frameType,
        final byte[] metadata,
        final byte[] data)
    {
        final int frameLength = computeFrameLength(frameType, metadata.length, data.length);

        if (INCLUDE_FRAME_LENGTH)
        {
            mutableDirectBuffer.putInt(FRAME_LENGTH_FIELD_OFFSET, frameLength, ByteOrder.BIG_ENDIAN);
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

        mutableDirectBuffer.putByte(VERSION_FIELD_OFFSET, CURRENT_VERSION);
        mutableDirectBuffer.putByte(FLAGS_FIELD_OFFSET, (byte) flags);
        mutableDirectBuffer.putShort(TYPE_FIELD_OFFSET, (short) outFrameType.getEncodedType(), ByteOrder.BIG_ENDIAN);
        mutableDirectBuffer.putLong(STREAM_ID_FIELD_OFFSET, streamId, ByteOrder.BIG_ENDIAN);
        mutableDirectBuffer.putBytes(offset, metadata);
        offset += metadataLength;
        mutableDirectBuffer.putBytes(offset, data);

        return frameLength;
    }

    public static int version(final DirectBuffer directBuffer)
    {
        return directBuffer.getByte(VERSION_FIELD_OFFSET);
    }

    public static int flags(final DirectBuffer directBuffer)
    {
        return directBuffer.getByte(FLAGS_FIELD_OFFSET);
    }

    public static FrameType frameType(final DirectBuffer directBuffer)
    {
        FrameType result = FrameType.from(directBuffer.getShort(TYPE_FIELD_OFFSET, ByteOrder.BIG_ENDIAN));
        final int dataLength = dataLength(directBuffer, 0);

        if (FrameType.RESPONSE == result)
        {
            final int flags = flags(directBuffer);

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

    public static long streamId(final DirectBuffer directBuffer)
    {
        return directBuffer.getLong(STREAM_ID_FIELD_OFFSET, ByteOrder.BIG_ENDIAN);
    }

    public static String frameData(final DirectBuffer directBuffer, final int length)
    {
        final int dataLength = dataLength(directBuffer, length);
        final int dataOffset = dataOffset(directBuffer);

        // allocat temporary
        final byte[] byteArray = new byte[dataLength];
        directBuffer.getBytes(dataOffset, byteArray, 0, dataLength);

        return new String(byteArray, 0, dataLength);
    }

    public static ByteBuffer sliceFrameData(final DirectBuffer directBuffer, final int length)
    {
        final int dataLength = dataLength(directBuffer, length);
        final int dataOffset = dataOffset(directBuffer);

        return slice(directBuffer.byteBuffer(), dataOffset, dataOffset + dataLength);
    }

    public static ByteBuffer sliceFrameMetadata(final DirectBuffer directBuffer, final int length)
    {
        final int metadataLength = metadataLength(directBuffer);
        final int metadataOffset = metadataOffset(directBuffer);

        return slice(directBuffer.byteBuffer(), metadataOffset, metadataOffset + metadataLength);
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

    private static int frameLength(final DirectBuffer directBuffer, final int externalFrameLength)
    {
        int frameLength = externalFrameLength;

        if (INCLUDE_FRAME_LENGTH)
        {
            frameLength = directBuffer.getInt(FRAME_LENGTH_FIELD_OFFSET, ByteOrder.BIG_ENDIAN);
        }

        return frameLength;
    }

    private static int metadataLength(final int metadataPayloadLength)
    {
        return metadataPayloadLength + ((0 == metadataPayloadLength) ? 0 : BitUtil.SIZE_OF_INT);
    }

    private static int metadataLength(final DirectBuffer directBuffer)
    {
        int metadataLength = 0;

        if (FLAGS_M == (FLAGS_M & directBuffer.getByte(FLAGS_FIELD_OFFSET)))
        {
            metadataLength = (directBuffer.getInt(metadataOffset(directBuffer)) & 0xFFFFFF) + BitUtil.SIZE_OF_INT;
        }

        return metadataLength;
    }

    private static int dataLength(final DirectBuffer directBuffer, final int length)
    {
        final int frameLength = frameLength(directBuffer, length);
        final int metadataLength = metadataLength(directBuffer);

        return frameLength - metadataLength - payloadOffset(directBuffer);
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

    private static int payloadOffset(final DirectBuffer directBuffer)
    {
        final FrameType frameType = FrameType.from(directBuffer.getShort(TYPE_FIELD_OFFSET, ByteOrder.BIG_ENDIAN));

        return payloadOffset(frameType);
    }

    private static int metadataOffset(final DirectBuffer directBuffer)
    {
        return payloadOffset(directBuffer);
    }

    private static int dataOffset(final DirectBuffer directBuffer)
    {
        return payloadOffset(directBuffer) + metadataLength(directBuffer);
    }
}
