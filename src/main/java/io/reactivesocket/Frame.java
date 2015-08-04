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

import java.nio.ByteBuffer;

/**
 * Represents a Frame sent over a {@link DuplexConnection}.
 * <p>
 * This provides encoding, decoding and field accessors.
 */
public class Frame
{
    // thread local as it needs to be single-threaded access
    private static final ThreadLocal<FrameFlyweight> FRAME_HANDLER = ThreadLocal.withInitial(FrameFlyweight::new);

    // not final so we can reuse this object
    private ByteBuffer byteBuffer;

    private Frame() {
    }

    /**
     * Return underlying {@link ByteBuffer} for frame
     *
     * @return underlying {@link ByteBuffer} for frame
     */
    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    /**
     * Return frame data as a String
     *
     * @return frame data as {@link System}
     */
    public String getData() {
        final FrameFlyweight frameFlyweight = FRAME_HANDLER.get();

        return frameFlyweight.framePayload(byteBuffer, 0);
    }

    /**
     * Return frame stream identifier
     *
     * @return frame stream identifier
     */
    public long getStreamId() {
        final FrameFlyweight frameFlyweight = FRAME_HANDLER.get();

        return frameFlyweight.streamId(byteBuffer);
    }

    /**
     * Return frame {@link FrameType}
     *
     * @return frame type
     */
    public FrameType getType() {
        final FrameFlyweight frameFlyweight = FRAME_HANDLER.get();

        return frameFlyweight.frameType(byteBuffer);
    }

    /**
     * Return frame version
     *
     * @return frame version
     */
    public int getVersion()
    {
        final FrameFlyweight frameFlyweight = FRAME_HANDLER.get();

        return frameFlyweight.version(byteBuffer);
    }

    /**
     * Return frame flags
     *
     * @return frame flags
     */
    public int getFlags()
    {
        final FrameFlyweight frameFlyweight = FRAME_HANDLER.get();

        return frameFlyweight.flags(byteBuffer);
    }

    /**
     * Mutates this Frame to contain the given ByteBuffer
     * 
     * @param byteBuffer to wrap
     */
    public void wrap(final ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    /**
     * Construct a new Frame from the given ByteBuffer
     * 
     * @param byteBuffer to wrap
     * @return new {@link Frame}
     */
    public static Frame from(final ByteBuffer byteBuffer) {
        Frame f = new Frame();
        f.byteBuffer = byteBuffer;
        return f;
    }

    /**
     * Mutates this Frame to contain the given parameters.
     *
     * NOTE: allocates a new {@link ByteBuffer}
     *
     * @param streamId to include in frame
     * @param type     to include in frame
     * @param message  to include in frame
     */
    public void wrap(final long streamId, final FrameType type, final String message) {
        this.byteBuffer = createByteBufferAndEncode(streamId, type, message);
    }

    /**
     * Construct a new Frame with the given parameters.
     * 
     * @param streamId to include in frame
     * @param type     to include in frame
     * @param message  to include in frame
     * @return         new {@link Frame}
     */
    public static Frame from(long streamId, FrameType type, String message) {
        Frame f = new Frame();
        f.byteBuffer = createByteBufferAndEncode(streamId, type, message);
        return f;
    }

    private static ByteBuffer createByteBufferAndEncode(long streamId, FrameType type, final String message) {
        final FrameFlyweight frameFlyweight = FRAME_HANDLER.get();

        // TODO: allocation side effect of how this works currently with the rest of the machinery.
        final ByteBuffer buffer = ByteBuffer.allocate(FrameFlyweight.computeFrameLength(message.length()));

        frameFlyweight.encode(buffer, streamId, type, message);
        return buffer;
    }

    @Override
    public String toString() {
        final FrameFlyweight frameFlyweight = FRAME_HANDLER.get();
        FrameType type = FrameType.SETUP;
        String payload = "";
        long streamId = -1;

        try
        {
            type = frameFlyweight.frameType(byteBuffer);
            payload = frameFlyweight.framePayload(byteBuffer, 0);
            streamId = frameFlyweight.streamId(byteBuffer);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "Frame => ID: " + streamId + " Type: " + type + " Data: " + payload;
    }
}
