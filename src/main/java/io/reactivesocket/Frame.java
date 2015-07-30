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

import uk.co.real_logic.agrona.DirectBuffer;

import java.nio.ByteBuffer;

/**
 * Represents a Frame sent over a {@link DuplexConnection}.
 * <p>
 * This provides encoding, decoding and field accessors.
 */
public class Frame
{
    private static final int INITIAL_MESSAGE_ARRAY_SIZE = 256;
    // TODO: make thread local to demonstrate idea
    private static final ThreadLocal<FrameFlyweight> FRAME_HANDLER = ThreadLocal.withInitial(FrameFlyweight::new);

    private Frame() {
    }

    // not final so we can reuse this object
    private ByteBuffer byteBuffer;
    private byte[] messageArray = new byte[INITIAL_MESSAGE_ARRAY_SIZE];
    private FrameType frameType;

    private long streamId;
    private int version;
    private int messageLength = 0;

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public String getMessage() {
        if (frameType == null) {
            decode();
        }
        return new String(messageArray, 0, messageLength);
    }

    public long getStreamId() {
        if (frameType == null) {
            decode();
        }
        return streamId;
    }

    public FrameType getMessageType() {
        if (frameType == null) {
            decode();
        }
        return frameType;
    }

    public int getVersion()
    {
        if (frameType == null)
        {
            decode();
        }
        return version;
    }

    /**
     * Mutates this Frame to contain the given ByteBuffer
     * 
     * @param b
     */
    public void wrap(ByteBuffer b) {
        this.streamId = -1;
        this.frameType = null;
        this.byteBuffer = b;
    }

    /**
     * Construct a new Frame from the given ByteBuffer
     * 
     * @param b
     * @return
     */
    public static Frame from(ByteBuffer b) {
        Frame f = new Frame();
        f.byteBuffer = b;
        return f;
    }

    /**
     * Mutates this Frame to contain the given message.
     * 
     * @param streamId
     * @param type
     * @param message
     */
    public void wrap(long streamId, FrameType type, String message) {
        this.streamId = streamId;
        this.frameType = type;

        final byte[] messageBytes = message.getBytes();
        ensureMessageArrayCapacity(messageBytes.length);

        System.arraycopy(messageBytes, 0, this.messageArray, 0, messageBytes.length);
        this.messageLength = messageBytes.length;

        this.byteBuffer = createByteBufferAndEncode(streamId, type, messageBytes);
    }

    public void setFromDecode(final int version, final long streamId, final FrameType type)
    {
        this.version = version;
        this.streamId = streamId;
        this.frameType = type;
    }

    public void setFromDecode(final DirectBuffer buffer, final int offset, final int messageLength)
    {
        ensureMessageArrayCapacity(messageLength);
        this.messageLength = messageLength;
        buffer.getBytes(offset, this.messageArray, 0, messageLength);
    }

    /**
     * Construct a new Frame with the given message.
     * 
     * @param streamId
     * @param type
     * @param message
     * @return
     */
    public static Frame from(long streamId, FrameType type, String message) {
        Frame f = new Frame();
        f.streamId = streamId;
        f.frameType = type;

        final byte[] messageBytes = message.getBytes();
        f.ensureMessageArrayCapacity(messageBytes.length);

        f.byteBuffer = createByteBufferAndEncode(streamId, type, messageBytes);

        System.arraycopy(messageBytes, 0, f.messageArray, 0, messageBytes.length);
        f.messageLength = messageBytes.length;
        return f;
    }

    private static ByteBuffer createByteBufferAndEncode(long streamId, FrameType type, final byte[] message) {
        final FrameFlyweight frameFlyweight = FRAME_HANDLER.get();

        // TODO: allocation side effect of how this works currently with the rest of the machinery.
        final ByteBuffer buffer = ByteBuffer.allocate(FrameFlyweight.frameLength(message.length));

        frameFlyweight.encode(buffer, streamId, type, message);
        return buffer;
    }

    private void decode() {
        final FrameFlyweight frameFlyweight = FRAME_HANDLER.get();

        frameFlyweight.decode(this, byteBuffer);
    }

    private void ensureMessageArrayCapacity(final int length)
    {
        if (messageArray.length < length)
        {
            messageArray = new byte[length];
        }
    }

    @Override
    public String toString() {
        if (frameType == null) {
            try {
                decode();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return "Frame => ID: " + streamId + " Type: " + frameType + " Data: " + getMessage();
    }
}
