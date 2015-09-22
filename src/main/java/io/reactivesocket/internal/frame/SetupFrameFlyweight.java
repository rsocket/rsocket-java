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
package io.reactivesocket.internal.frame;

import io.reactivesocket.FrameType;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;

public class SetupFrameFlyweight
{
    public static final int FLAGS_WILL_HONOR_LEASE = 0b0010_0000;
    public static final int FLAGS_STRICT_INTERPRETATION = 0b0001_0000;

    public static final byte CURRENT_VERSION = 0;

    // relative to start of passed offset
    private static final int VERSION_FIELD_OFFSET = FrameHeaderFlyweight.FRAME_HEADER_LENGTH;
    private static final int KEEPALIVE_INTERVAL_FIELD_OFFSET = VERSION_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
    private static final int MAX_LIFETIME_FIELD_OFFSET = KEEPALIVE_INTERVAL_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
    private static final int METADATA_MIME_TYPE_LENGTH_OFFSET = MAX_LIFETIME_FIELD_OFFSET + BitUtil.SIZE_OF_INT;

    public static int computeFrameLength(
        final String metadataMimeType,
        final String dataMimeType,
        final int metadataLength,
        final int dataLength)
    {
        int length = FrameHeaderFlyweight.computeFrameHeaderLength(FrameType.SETUP, metadataLength, dataLength);

        length += BitUtil.SIZE_OF_INT * 3;
        length += 1 + metadataMimeType.length();
        length += 1 + dataMimeType.length();

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
        final ByteBuffer data)
    {
        final int frameLength = computeFrameLength(metadataMimeType, dataMimeType, metadata.remaining(), data.remaining());

        int length = FrameHeaderFlyweight.encodeFrameHeader(mutableDirectBuffer, offset, frameLength, flags, FrameType.SETUP, 0);

        mutableDirectBuffer.putInt(offset + VERSION_FIELD_OFFSET, CURRENT_VERSION, ByteOrder.BIG_ENDIAN);
        mutableDirectBuffer.putInt(offset + KEEPALIVE_INTERVAL_FIELD_OFFSET, keepaliveInterval, ByteOrder.BIG_ENDIAN);
        mutableDirectBuffer.putInt(offset + MAX_LIFETIME_FIELD_OFFSET, maxLifetime, ByteOrder.BIG_ENDIAN);

        length += BitUtil.SIZE_OF_INT * 3;

        length += putMimeType(mutableDirectBuffer, offset + length, metadataMimeType);
        length += putMimeType(mutableDirectBuffer, offset + length, dataMimeType);

        length += FrameHeaderFlyweight.encodeMetadata(mutableDirectBuffer, offset, offset + length, metadata);
        length += FrameHeaderFlyweight.encodeData(mutableDirectBuffer, offset + length, data);

        return length;
    }

    public static int version(final DirectBuffer directBuffer, final int offset)
    {
        return directBuffer.getInt(offset + VERSION_FIELD_OFFSET, ByteOrder.BIG_ENDIAN);
    }

    public static int keepaliveInterval(final DirectBuffer directBuffer, final int offset)
    {
        return directBuffer.getInt(offset + KEEPALIVE_INTERVAL_FIELD_OFFSET, ByteOrder.BIG_ENDIAN);
    }

    public static int maxLifetime(final DirectBuffer directBuffer, final int offset)
    {
        return directBuffer.getInt(offset + MAX_LIFETIME_FIELD_OFFSET, ByteOrder.BIG_ENDIAN);
    }

    public static String metadataMimeType(final DirectBuffer directBuffer, final int offset)
    {
        final byte[] bytes = getMimeType(directBuffer, offset + METADATA_MIME_TYPE_LENGTH_OFFSET);
        return new String(bytes, Charset.forName("UTF-8"));
    }

    public static String dataMimeType(final DirectBuffer directBuffer, final int offset)
    {
        int fieldOffset = offset + METADATA_MIME_TYPE_LENGTH_OFFSET;

        fieldOffset += 1 + directBuffer.getByte(fieldOffset);

        final byte[] bytes = getMimeType(directBuffer, fieldOffset);
        return new String(bytes, Charset.forName("UTF-8"));
    }

    public static int computePayloadOffset(
        final int offset, final int metadataMimeTypeLength, final int dataMimeTypeLength)
    {
        return offset + METADATA_MIME_TYPE_LENGTH_OFFSET +
            1 + metadataMimeTypeLength +
            1 + dataMimeTypeLength;
    }

    public static int payloadOffset(final DirectBuffer directBuffer, final int offset)
    {
        int fieldOffset = offset + METADATA_MIME_TYPE_LENGTH_OFFSET;

        final int metadataMimeTypeLength = directBuffer.getByte(fieldOffset);
        fieldOffset += 1 + metadataMimeTypeLength;

        final int dataMimeTypeLength = directBuffer.getByte(fieldOffset);
        fieldOffset += 1 + dataMimeTypeLength;

        return fieldOffset;
    }

    private static int putMimeType(
        final MutableDirectBuffer mutableDirectBuffer, final int fieldOffset, final String mimeType)
    {
        mutableDirectBuffer.putByte(fieldOffset, (byte) mimeType.length());
        mutableDirectBuffer.putBytes(fieldOffset + 1, mimeType.getBytes());

        return 1 + mimeType.length();
    }

    private static byte[] getMimeType(final DirectBuffer directBuffer, final int fieldOffset)
    {
        final int length = directBuffer.getByte(fieldOffset);
        final byte[] bytes = new byte[length];

        directBuffer.getBytes(fieldOffset + 1, bytes);
        return bytes;
    }
}
