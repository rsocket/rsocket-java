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
import java.nio.ByteOrder;

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
public class FrameHeaderFlyweight {

    private FrameHeaderFlyweight() {}

    public static final ByteBuffer NULL_BYTEBUFFER = ByteBuffer.allocate(0);

    public static final int FRAME_HEADER_LENGTH;

    private static final boolean INCLUDE_FRAME_LENGTH = true;

    private static final int FRAME_TYPE_BITS = 6;
    private static final int FRAME_TYPE_SHIFT = 16 - FRAME_TYPE_BITS;
    private static final int FRAME_FLAGS_MASK = 0b0000_0011_1111_1111;

    private static final int FRAME_LENGTH_FIELD_OFFSET;
    private static final int FRAME_TYPE_AND_FLAGS_FIELD_OFFSET;
    private static final int STREAM_ID_FIELD_OFFSET;
    private static final int PAYLOAD_OFFSET;

    public static final int FLAGS_I = 0b10_0000_0000;
    public static final int FLAGS_M = 0b01_0000_0000;

    // TODO(lexs): These are frame specific and should not live here
    public static final int FLAGS_KEEPALIVE_R = 0b00_1000_0000;

    public static final int FLAGS_RESPONSE_F = 0b00_1000_0000;
    public static final int FLAGS_RESPONSE_C = 0b00_0100_0000;

    public static final int FLAGS_REQUEST_CHANNEL_F = 0b00_1000_0000;

    static {
        if (INCLUDE_FRAME_LENGTH) {
            FRAME_LENGTH_FIELD_OFFSET = 0;
        } else {
            FRAME_LENGTH_FIELD_OFFSET = -BitUtil.SIZE_OF_INT;
        }

        STREAM_ID_FIELD_OFFSET = FRAME_LENGTH_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
        FRAME_TYPE_AND_FLAGS_FIELD_OFFSET = STREAM_ID_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
        PAYLOAD_OFFSET = FRAME_TYPE_AND_FLAGS_FIELD_OFFSET + BitUtil.SIZE_OF_SHORT;

        FRAME_HEADER_LENGTH = PAYLOAD_OFFSET;
    }

    public static int computeFrameHeaderLength(final FrameType frameType, int metadataLength, final int dataLength) {
        return PAYLOAD_OFFSET + computeMetadataLength(metadataLength) + dataLength;
    }

    public static int encodeFrameHeader(
            final MutableDirectBuffer mutableDirectBuffer,
            final int offset,
            final int frameLength,
            final int flags,
            final FrameType frameType,
            final int streamId
    ) {
        if (INCLUDE_FRAME_LENGTH) {
            mutableDirectBuffer.putInt(offset + FRAME_LENGTH_FIELD_OFFSET, frameLength, ByteOrder.BIG_ENDIAN);
        }

        mutableDirectBuffer.putInt(offset + STREAM_ID_FIELD_OFFSET, streamId, ByteOrder.BIG_ENDIAN);
        short typeAndFlags = (short) (frameType.getEncodedType() << FRAME_TYPE_SHIFT | (short) flags);
        mutableDirectBuffer.putShort(offset + FRAME_TYPE_AND_FLAGS_FIELD_OFFSET, typeAndFlags, ByteOrder.BIG_ENDIAN);

        return FRAME_HEADER_LENGTH;
    }

    public static int encodeMetadata(
            final MutableDirectBuffer mutableDirectBuffer,
            final int frameHeaderStartOffset,
            final int metadataOffset,
            final ByteBuffer metadata
    ) {
        int length = 0;
        final int metadataLength = metadata.remaining();

        if (0 < metadataLength) {
            int typeAndFlags = mutableDirectBuffer.getShort(frameHeaderStartOffset + FRAME_TYPE_AND_FLAGS_FIELD_OFFSET, ByteOrder.BIG_ENDIAN);
            typeAndFlags |= FLAGS_M;
            mutableDirectBuffer.putShort(frameHeaderStartOffset + FRAME_TYPE_AND_FLAGS_FIELD_OFFSET, (short) typeAndFlags, ByteOrder.BIG_ENDIAN);
            mutableDirectBuffer.putInt(metadataOffset, metadataLength + BitUtil.SIZE_OF_INT, ByteOrder.BIG_ENDIAN);
            length += BitUtil.SIZE_OF_INT;
            mutableDirectBuffer.putBytes(metadataOffset + length, metadata, metadataLength);
            length += metadataLength;
        }

        return length;
    }

    public static int encodeData(
            final MutableDirectBuffer mutableDirectBuffer,
            final int dataOffset,
            final ByteBuffer data
    ) {
        int length = 0;
        final int dataLength = data.remaining();

        if (0 < dataLength) {
            mutableDirectBuffer.putBytes(dataOffset, data, dataLength);
            length += dataLength;
        }

        return length;
    }

    // only used for types simple enough that they don't have their own FrameFlyweights
    public static int encode(
            final MutableDirectBuffer mutableDirectBuffer,
            final int offset,
            final int streamId,
            int flags,
            final FrameType frameType,
            final ByteBuffer metadata,
            final ByteBuffer data
    ) {
        final int frameLength = computeFrameHeaderLength(frameType, metadata.remaining(), data.remaining());

        final FrameType outFrameType;
        switch (frameType) {
            case NEXT_COMPLETE:
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

        int length = encodeFrameHeader(mutableDirectBuffer, offset, frameLength, flags, outFrameType, streamId);

        length += encodeMetadata(mutableDirectBuffer, offset, offset + length, metadata);
        length += encodeData(mutableDirectBuffer, offset + length, data);

        return length;
    }

    public static int flags(final DirectBuffer directBuffer, final int offset) {
        short typeAndFlags = directBuffer.getShort(offset + FRAME_TYPE_AND_FLAGS_FIELD_OFFSET, ByteOrder.BIG_ENDIAN);
        return typeAndFlags & FRAME_FLAGS_MASK;
    }

    public static FrameType frameType(final DirectBuffer directBuffer, final int offset) {
        int typeAndFlags = directBuffer.getShort(offset + FRAME_TYPE_AND_FLAGS_FIELD_OFFSET, ByteOrder.BIG_ENDIAN);
        FrameType result = FrameType.from(typeAndFlags >> FRAME_TYPE_SHIFT);

        if (FrameType.RESPONSE == result) {
            // FIXME
            final int flags = flags(directBuffer, offset);

            boolean complete = FLAGS_RESPONSE_C == (flags & FLAGS_RESPONSE_C);
            if (complete) {
                result = FrameType.NEXT_COMPLETE;
            } else {
                result = FrameType.NEXT;
            }
        }

        return result;
    }

    public static int streamId(final DirectBuffer directBuffer, final int offset) {
        return directBuffer.getInt(offset + STREAM_ID_FIELD_OFFSET, ByteOrder.BIG_ENDIAN);
    }

    public static ByteBuffer sliceFrameData(final DirectBuffer directBuffer, final int offset, final int length) {
        final int dataLength = dataLength(directBuffer, offset, length);
        final int dataOffset = dataOffset(directBuffer, offset);
        ByteBuffer result = NULL_BYTEBUFFER;

        if (0 < dataLength) {
            result = ByteBufferUtil.preservingSlice(directBuffer.byteBuffer(), dataOffset, dataOffset + dataLength);
        }

        return result;
    }

    public static ByteBuffer sliceFrameMetadata(final DirectBuffer directBuffer, final int offset, final int length) {
        final int metadataLength = Math.max(0, metadataFieldLength(directBuffer, offset) - BitUtil.SIZE_OF_INT);
        final int metadataOffset = metadataOffset(directBuffer, offset) + BitUtil.SIZE_OF_INT;
        ByteBuffer result = NULL_BYTEBUFFER;

        if (0 < metadataLength) {
            result = ByteBufferUtil.preservingSlice(directBuffer.byteBuffer(), metadataOffset, metadataOffset + metadataLength);
        }

        return result;
    }

    private static int frameLength(final DirectBuffer directBuffer, final int offset, final int externalFrameLength) {
        int frameLength = externalFrameLength;

        if (INCLUDE_FRAME_LENGTH) {
            frameLength = directBuffer.getInt(offset + FRAME_LENGTH_FIELD_OFFSET, ByteOrder.BIG_ENDIAN);
        }

        return frameLength;
    }

    private static int computeMetadataLength(final int metadataPayloadLength) {
        return metadataPayloadLength + (0 == metadataPayloadLength? 0 : BitUtil.SIZE_OF_INT);
    }

    private static int metadataFieldLength(final DirectBuffer directBuffer, final int offset) {
        int metadataLength = 0;

        int flags = flags(directBuffer, offset);
        if (FLAGS_M == (FLAGS_M & flags)) {
            metadataLength = directBuffer.getInt(metadataOffset(directBuffer, offset), ByteOrder.BIG_ENDIAN) & 0xFFFFFF;
        }

        return metadataLength;
    }

    private static int dataLength(final DirectBuffer directBuffer, final int offset, final int externalLength) {
        final int frameLength = frameLength(directBuffer, offset, externalLength);
        final int metadataLength = metadataFieldLength(directBuffer, offset);

        return offset + frameLength - metadataLength - payloadOffset(directBuffer, offset);
    }

    private static int payloadOffset(final DirectBuffer directBuffer, final int offset) {
        int typeAndFlags = directBuffer.getShort(offset + FRAME_TYPE_AND_FLAGS_FIELD_OFFSET, ByteOrder.BIG_ENDIAN);
        FrameType frameType = FrameType.from(typeAndFlags >> FRAME_TYPE_SHIFT);
        int result = offset + PAYLOAD_OFFSET;

        switch (frameType) {
            case SETUP:
                result = SetupFrameFlyweight.payloadOffset(directBuffer, offset);
                break;
            case ERROR:
                result = ErrorFrameFlyweight.payloadOffset(directBuffer, offset);
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
            case REQUEST_N:
                result = RequestNFrameFlyweight.payloadOffset(directBuffer, offset);
                break;
        }

        return result;
    }

    private static int metadataOffset(final DirectBuffer directBuffer, final int offset) {
        return payloadOffset(directBuffer, offset);
    }

    private static int dataOffset(final DirectBuffer directBuffer, final int offset) {
        return payloadOffset(directBuffer, offset) + metadataFieldLength(directBuffer, offset);
    }
}
