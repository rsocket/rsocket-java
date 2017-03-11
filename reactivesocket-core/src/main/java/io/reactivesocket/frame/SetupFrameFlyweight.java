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
import java.nio.charset.StandardCharsets;

public class SetupFrameFlyweight {
    private SetupFrameFlyweight() {}

    public static final int FLAGS_RESUME_ENABLE = 0b00_1000_0000;
    public static final int FLAGS_WILL_HONOR_LEASE = 0b00_0100_0000;
    public static final int FLAGS_STRICT_INTERPRETATION = 0b00_0010_0000;

    public static final int VALID_FLAGS = FLAGS_RESUME_ENABLE | FLAGS_WILL_HONOR_LEASE | FLAGS_STRICT_INTERPRETATION;

    public static final int CURRENT_VERSION = VersionFlyweight.encode(1, 0);

    // relative to start of passed offset
    private static final int VERSION_FIELD_OFFSET = FrameHeaderFlyweight.FRAME_HEADER_LENGTH;
    private static final int KEEPALIVE_INTERVAL_FIELD_OFFSET = VERSION_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
    private static final int MAX_LIFETIME_FIELD_OFFSET = KEEPALIVE_INTERVAL_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
    private static final int VARIABLE_DATA_OFFSET = MAX_LIFETIME_FIELD_OFFSET + BitUtil.SIZE_OF_INT;

    public static int computeFrameLength(
        final int flags,
        final String metadataMimeType,
        final String dataMimeType,
        final int metadataLength,
        final int dataLength
    ) {
        return computeFrameLength(flags, 0, metadataMimeType, dataMimeType, metadataLength, dataLength);
    }

    private static int computeFrameLength(
        final int flags,
        final int resumeTokenLength,
        final String metadataMimeType,
        final String dataMimeType,
        final int metadataLength,
        final int dataLength
    ) {
        int length = FrameHeaderFlyweight.computeFrameHeaderLength(FrameType.SETUP, metadataLength, dataLength);

        length += BitUtil.SIZE_OF_INT * 3;

        if ((flags & FLAGS_RESUME_ENABLE) != 0) {
            length += BitUtil.SIZE_OF_SHORT + resumeTokenLength;
        }

        length += 1 + metadataMimeType.getBytes(StandardCharsets.UTF_8).length;
        length += 1 + dataMimeType.getBytes(StandardCharsets.UTF_8).length;

        return length;
    }

    public static int encode(
        final MutableDirectBuffer mutableDirectBuffer,
        final int offset,
        int flags,
        final int keepaliveInterval,
        final int maxLifetime,
        final String metadataMimeType,
        final String dataMimeType,
        final ByteBuffer metadata,
        final ByteBuffer data
    ) {
        if ((flags & FLAGS_RESUME_ENABLE) != 0) {
            throw new IllegalArgumentException("RESUME_ENABLE not supported");
        }

        return encode(
                mutableDirectBuffer,
                offset,
                flags,
                keepaliveInterval,
                maxLifetime,
                FrameHeaderFlyweight.NULL_BYTEBUFFER,
                metadataMimeType,
                dataMimeType,
                metadata,
                data);
    }

    // Only exposed for testing, other code shouldn't create frames with resumption tokens for now
    static int encode(
        final MutableDirectBuffer mutableDirectBuffer,
        final int offset,
        int flags,
        final int keepaliveInterval,
        final int maxLifetime,
        final ByteBuffer resumeToken,
        final String metadataMimeType,
        final String dataMimeType,
        final ByteBuffer metadata,
        final ByteBuffer data
    ) {
        final int frameLength = computeFrameLength(flags, resumeToken.remaining(), metadataMimeType, dataMimeType, metadata.remaining(), data.remaining());

        int length = FrameHeaderFlyweight.encodeFrameHeader(mutableDirectBuffer, offset, frameLength, flags, FrameType.SETUP, 0);

        mutableDirectBuffer.putInt(offset + VERSION_FIELD_OFFSET, CURRENT_VERSION, ByteOrder.BIG_ENDIAN);
        mutableDirectBuffer.putInt(offset + KEEPALIVE_INTERVAL_FIELD_OFFSET, keepaliveInterval, ByteOrder.BIG_ENDIAN);
        mutableDirectBuffer.putInt(offset + MAX_LIFETIME_FIELD_OFFSET, maxLifetime, ByteOrder.BIG_ENDIAN);

        length += BitUtil.SIZE_OF_INT * 3;

        if ((flags & FLAGS_RESUME_ENABLE) != 0) {
            mutableDirectBuffer.putShort(offset + length, (short) resumeToken.remaining(), ByteOrder.BIG_ENDIAN);
            length += BitUtil.SIZE_OF_SHORT;
            int resumeTokenLength = resumeToken.remaining();
            mutableDirectBuffer.putBytes(offset + length, resumeToken, resumeTokenLength);
            length += resumeTokenLength;
        }

        length += putMimeType(mutableDirectBuffer, offset + length, metadataMimeType);
        length += putMimeType(mutableDirectBuffer, offset + length, dataMimeType);

        length += FrameHeaderFlyweight.encodeMetadata(
                mutableDirectBuffer, FrameType.SETUP, offset, offset + length, metadata);
        length += FrameHeaderFlyweight.encodeData(mutableDirectBuffer, offset + length, data);

        return length;
    }

    public static int version(final DirectBuffer directBuffer, final int offset) {
        return directBuffer.getInt(offset + VERSION_FIELD_OFFSET, ByteOrder.BIG_ENDIAN);
    }

    public static int keepaliveInterval(final DirectBuffer directBuffer, final int offset) {
        return directBuffer.getInt(offset + KEEPALIVE_INTERVAL_FIELD_OFFSET, ByteOrder.BIG_ENDIAN);
    }

    public static int maxLifetime(final DirectBuffer directBuffer, final int offset) {
        return directBuffer.getInt(offset + MAX_LIFETIME_FIELD_OFFSET, ByteOrder.BIG_ENDIAN);
    }

    public static String metadataMimeType(final DirectBuffer directBuffer, final int offset) {
        final byte[] bytes = getMimeType(directBuffer, offset + metadataMimetypeOffset(directBuffer, offset));
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static String dataMimeType(final DirectBuffer directBuffer, final int offset) {
        int fieldOffset = offset + metadataMimetypeOffset(directBuffer, offset);

        fieldOffset += 1 + directBuffer.getByte(fieldOffset);

        final byte[] bytes = getMimeType(directBuffer, fieldOffset);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static int payloadOffset(final DirectBuffer directBuffer, final int offset) {
        int fieldOffset = offset + metadataMimetypeOffset(directBuffer, offset);

        final int metadataMimeTypeLength = directBuffer.getByte(fieldOffset);
        fieldOffset += 1 + metadataMimeTypeLength;

        final int dataMimeTypeLength = directBuffer.getByte(fieldOffset);
        fieldOffset += 1 + dataMimeTypeLength;

        return fieldOffset;
    }

    private static int metadataMimetypeOffset(final DirectBuffer directBuffer, final int offset) {
        return VARIABLE_DATA_OFFSET + resumeTokenTotalLength(directBuffer, offset);
    }

    private static int resumeTokenTotalLength(final DirectBuffer directBuffer, final int offset) {
        if ((FrameHeaderFlyweight.flags(directBuffer, offset) & FLAGS_RESUME_ENABLE) == 0) {
            return 0;
        } else {
            return BitUtil.SIZE_OF_SHORT + directBuffer.getShort(offset + VARIABLE_DATA_OFFSET, ByteOrder.BIG_ENDIAN);
        }
    }

    private static int putMimeType(
        final MutableDirectBuffer mutableDirectBuffer, final int fieldOffset, final String mimeType) {
        byte[] bytes = mimeType.getBytes(StandardCharsets.UTF_8);

        mutableDirectBuffer.putByte(fieldOffset, (byte) bytes.length);
        mutableDirectBuffer.putBytes(fieldOffset + 1, bytes);

        return 1 + bytes.length;
    }

    private static byte[] getMimeType(final DirectBuffer directBuffer, final int fieldOffset) {
        final int length = directBuffer.getByte(fieldOffset);
        final byte[] bytes = new byte[length];

        directBuffer.getBytes(fieldOffset + 1, bytes);
        return bytes;
    }
}
