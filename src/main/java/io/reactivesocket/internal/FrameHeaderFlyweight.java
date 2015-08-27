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

import static io.reactivesocket.internal.ByteBufferUtil.preservingSlice;

/**
 * Per connection frame flyweight.
 *
 * Not the latest frame layout, but close.
 * Does not include
 * - fragmentation / reassembly
 * - encode should remove Type param and have it as part of method name (1 encode per type?)
 *
 * Not thread-safe. Assumed to be used single-threaded
 */
public class FrameHeaderFlyweight
{
    public static final ByteBuffer NULL_BYTEBUFFER = ByteBuffer.allocate(0);

    public static final int FRAME_HEADER_LENGTH;

    private static final boolean INCLUDE_FRAME_LENGTH = true;

    private static final int FRAME_LENGTH_FIELD_OFFSET;
    private static final int VERSION_FIELD_OFFSET;
    private static final int FLAGS_FIELD_OFFSET;
    private static final int TYPE_FIELD_OFFSET;
    private static final int STREAM_ID_FIELD_OFFSET;
    private static final int PAYLOAD_OFFSET;

    private static final byte CURRENT_VERSION = 0;

    public static final int FLAGS_I = 0b1000_0000;
    public static final int FLAGS_M = 0b0100_0000;

    private static final int FLAGS_RESPONSE_F = 0b0010_0000;
    private static final int FLAGS_RESPONSE_C = 0b0001_0000;

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

        FRAME_HEADER_LENGTH = PAYLOAD_OFFSET;
    }

    public static int computeFrameHeaderLength(final FrameType frameType, int metadataLength, final int dataLength)
    {
        return PAYLOAD_OFFSET + computeMetadataLength(metadataLength) + dataLength;
    }

    public static int encodeFrameHeader(
        final MutableDirectBuffer mutableDirectBuffer,
        final int offset,
        final int frameLength,
        final int flags,
        final FrameType frameType,
        final long streamId)
    {
        if (INCLUDE_FRAME_LENGTH)
        {
            mutableDirectBuffer.putInt(offset + FRAME_LENGTH_FIELD_OFFSET, frameLength, ByteOrder.BIG_ENDIAN);
        }

        mutableDirectBuffer.putByte(offset + VERSION_FIELD_OFFSET, CURRENT_VERSION);
        mutableDirectBuffer.putByte(offset + FLAGS_FIELD_OFFSET, (byte) flags);
        mutableDirectBuffer.putShort(offset + TYPE_FIELD_OFFSET, (short) frameType.getEncodedType(), ByteOrder.BIG_ENDIAN);
        mutableDirectBuffer.putLong(offset + STREAM_ID_FIELD_OFFSET, streamId, ByteOrder.BIG_ENDIAN);

        return FRAME_HEADER_LENGTH;
    }

    public static int encodeMetadata(
        final MutableDirectBuffer mutableDirectBuffer,
        final int frameHeaderStartOffset,
        final int metadataOffset,
        final ByteBuffer metadata)
    {
        int length = 0;

        if (0 < metadata.capacity())
        {
            int flags = mutableDirectBuffer.getByte(frameHeaderStartOffset + FLAGS_FIELD_OFFSET);
            flags |= FLAGS_M;
            mutableDirectBuffer.putByte(frameHeaderStartOffset + FLAGS_FIELD_OFFSET, (byte)flags);
            mutableDirectBuffer.putInt(metadataOffset, metadata.capacity() + BitUtil.SIZE_OF_INT, ByteOrder.BIG_ENDIAN);
            length += BitUtil.SIZE_OF_INT;
            mutableDirectBuffer.putBytes(metadataOffset + length, metadata, metadata.capacity());
            length += metadata.capacity();
        }

        return length;
    }

    public static int encodeData(
        final MutableDirectBuffer mutableDirectBuffer,
        final int dataOffset,
        final ByteBuffer data)
    {
        int length = 0;

        if (0 < data.capacity())
        {
            mutableDirectBuffer.putBytes(dataOffset, data, data.capacity());
            length += data.capacity();
        }

        return length;
    }

    // only used for types simple enough that they don't have their own FrameFlyweights
    public static int encode(
        final MutableDirectBuffer mutableDirectBuffer,
        final int offset,
        final long streamId,
        final FrameType frameType,
        final ByteBuffer metadata,
        final ByteBuffer data)
    {
        final int frameLength = computeFrameHeaderLength(frameType, metadata.capacity(), data.capacity());

        final FrameType outFrameType;
        int flags = 0;

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

        int length = FrameHeaderFlyweight.encodeFrameHeader(mutableDirectBuffer, offset, frameLength, flags, outFrameType, streamId);

        length += FrameHeaderFlyweight.encodeMetadata(mutableDirectBuffer, offset, offset + length, metadata);
        length += FrameHeaderFlyweight.encodeData(mutableDirectBuffer, offset + length, data);

        return length;
    }

    public static int version(final DirectBuffer directBuffer, final int offset)
    {
        return directBuffer.getByte(offset + VERSION_FIELD_OFFSET);
    }

    public static int flags(final DirectBuffer directBuffer, final int offset)
    {
        return directBuffer.getByte(offset + FLAGS_FIELD_OFFSET);
    }

    public static FrameType frameType(final DirectBuffer directBuffer, final int offset)
    {
        FrameType result = FrameType.from(directBuffer.getShort(offset + TYPE_FIELD_OFFSET, ByteOrder.BIG_ENDIAN));

        if (FrameType.RESPONSE == result)
        {
            final int flags = flags(directBuffer, offset);
            final int dataLength = dataLength(directBuffer, offset, 0);

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

    public static long streamId(final DirectBuffer directBuffer, final int offset)
    {
        return directBuffer.getLong(offset + STREAM_ID_FIELD_OFFSET, ByteOrder.BIG_ENDIAN);
    }

    public static ByteBuffer sliceFrameData(final DirectBuffer directBuffer, final int offset, final int length)
    {
        final int dataLength = dataLength(directBuffer, offset, length);
        final int dataOffset = dataOffset(directBuffer, offset);
        ByteBuffer result = NULL_BYTEBUFFER;

        if (0 < dataLength)
        {
            result = preservingSlice(directBuffer.byteBuffer(), dataOffset, dataOffset + dataLength);
        }

        return result;
    }

    public static ByteBuffer sliceFrameMetadata(final DirectBuffer directBuffer, final int offset, final int length)
    {
        final int metadataLength = Math.max(0, metadataFieldLength(directBuffer, offset) - BitUtil.SIZE_OF_INT);
        final int metadataOffset = metadataOffset(directBuffer, offset) + BitUtil.SIZE_OF_INT;
        ByteBuffer result = NULL_BYTEBUFFER;

        if (0 < metadataLength)
        {
            result = preservingSlice(directBuffer.byteBuffer(), metadataOffset, metadataOffset + metadataLength);
        }

        return result;
    }

    private static int frameLength(final DirectBuffer directBuffer, final int offset, final int externalFrameLength)
    {
        int frameLength = externalFrameLength;

        if (INCLUDE_FRAME_LENGTH)
        {
            frameLength = directBuffer.getInt(offset + FRAME_LENGTH_FIELD_OFFSET, ByteOrder.BIG_ENDIAN);
        }

        return frameLength;
    }

    private static int computeMetadataLength(final int metadataPayloadLength)
    {
        return metadataPayloadLength + ((0 == metadataPayloadLength) ? 0 : BitUtil.SIZE_OF_INT);
    }

    private static int metadataFieldLength(final DirectBuffer directBuffer, final int offset)
    {
        int metadataLength = 0;

        if (FLAGS_M == (FLAGS_M & directBuffer.getByte(offset + FLAGS_FIELD_OFFSET)))
        {
            metadataLength = (directBuffer.getInt(metadataOffset(directBuffer, offset), ByteOrder.BIG_ENDIAN) & 0xFFFFFF);
        }

        return metadataLength;
    }

    private static int dataLength(final DirectBuffer directBuffer, final int offset, final int externalLength)
    {
        final int frameLength = frameLength(directBuffer, offset, externalLength);
        final int metadataLength = metadataFieldLength(directBuffer, offset);

        return frameLength - metadataLength - payloadOffset(directBuffer, offset);
    }

    private static int payloadOffset(final DirectBuffer directBuffer, final int offset)
    {
        final FrameType frameType = FrameType.from(directBuffer.getShort(offset + TYPE_FIELD_OFFSET, ByteOrder.BIG_ENDIAN));
        int result = PAYLOAD_OFFSET;

        switch (frameType)
        {
            case SETUP:
                result = SetupFrameFlyweight.payloadOffset(directBuffer, offset);
                break;
            case SETUP_ERROR:
                result = SetupErrorFrameFlyweight.payloadOffset(directBuffer, offset);
                break;
            case LEASE:
                result = LeaseFrameFlyweight.payloadOffset(directBuffer, offset);
                break;
            case KEEPALIVE:
                result = KeepaliveFrameFlyweight.payloadOffset(directBuffer, offset);
                break;
            case REQUEST_RESPONSE:
            case FIRE_AND_FORGET:
            case REQUEST_STREAM:
            case REQUEST_SUBSCRIPTION:
            case REQUEST_CHANNEL:
                result = RequestFrameFlyweight.payloadOffset(frameType, directBuffer, offset);
                break;
        }

        return result;
    }

    private static int metadataOffset(final DirectBuffer directBuffer, final int offset)
    {
        return payloadOffset(directBuffer, offset);
    }

    private static int dataOffset(final DirectBuffer directBuffer, final int offset)
    {
        return payloadOffset(directBuffer, offset) + metadataFieldLength(directBuffer, offset);
    }
}
