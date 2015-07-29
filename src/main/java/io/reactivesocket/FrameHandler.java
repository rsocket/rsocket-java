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
 * Per connection frame handling.
 * Holds codecs and DirectBuffers for wrapping
 */
public class FrameHandler
{
    /**
     * Not the real frame layout, just an iteration on the ASCII version
     */
    private static final int VERSION_FIELD_OFFSET = 0;
    private static final int STREAM_ID_FIELD_OFFSET = VERSION_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
    private static final int TYPE_FIELD_OFFSET = STREAM_ID_FIELD_OFFSET + BitUtil.SIZE_OF_LONG;
    private static final int DATA_LENGTH_OFFSET = TYPE_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
    private static final int DATA_OFFSET = DATA_LENGTH_OFFSET + BitUtil.SIZE_OF_INT;

    private static final byte CURRENT_VERSION = 0;

    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    // single threaded assumed
    private final MutableDirectBuffer frameBuffer = new UnsafeBuffer(EMPTY_BUFFER);

    // set by decode
    private long streamId;
    private MessageType messageType;
    private byte version;
    private int dataLength;

    public static int frameLength(final int dataLength)
    {
        return DATA_OFFSET + dataLength;
    }

    public void encode(final ByteBuffer byteBuffer, final long streamId, final MessageType type, final byte[] data)
    {
        frameBuffer.wrap(byteBuffer);
        frameBuffer.putByte(VERSION_FIELD_OFFSET, CURRENT_VERSION);
        frameBuffer.putLong(STREAM_ID_FIELD_OFFSET, streamId, ByteOrder.BIG_ENDIAN);
        frameBuffer.putInt(TYPE_FIELD_OFFSET, type.getMessageId(), ByteOrder.BIG_ENDIAN);
        frameBuffer.putInt(DATA_LENGTH_OFFSET, data.length, ByteOrder.BIG_ENDIAN);
        frameBuffer.putBytes(DATA_OFFSET, data);
    }

    /**
     * populate streamId, type, dataLength, etc.
     */
    public void decode(final ByteBuffer byteBuffer)
    {
        frameBuffer.wrap(byteBuffer);

        version = frameBuffer.getByte(VERSION_FIELD_OFFSET);
        streamId = frameBuffer.getLong(STREAM_ID_FIELD_OFFSET, ByteOrder.BIG_ENDIAN);
        messageType = MessageType.from(frameBuffer.getInt(TYPE_FIELD_OFFSET, ByteOrder.BIG_ENDIAN));
        dataLength = frameBuffer.getInt(DATA_LENGTH_OFFSET, ByteOrder.BIG_ENDIAN);
    }

    public ByteBuffer buffer()
    {
        return frameBuffer.byteBuffer();
    }

    public byte version()
    {
        return version;
    }

    public MessageType messageType()
    {
        return messageType;
    }

    public long streamId()
    {
        return streamId;
    }

    public int dataLength()
    {
        return dataLength;
    }

    public void getDataBytes(final byte[] array)
    {
        frameBuffer.getBytes(DATA_OFFSET, array);
    }

}
