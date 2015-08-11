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

import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

/**
 * Represents a Frame sent over a {@link DuplexConnection}.
 * <p>
 * This provides encoding, decoding and field accessors.
 */
public class Frame
{
    // thread local as it needs to be single-threaded access
//    private static final ThreadLocal<FrameFlyweight> FRAME_HANDLER = ThreadLocal.withInitial(FrameFlyweight::new);

    // not final so we can reuse this object
    private MutableDirectBuffer directBuffer;

    private Frame() {
    }

    /**
     * Return underlying {@link ByteBuffer} for frame
     *
     * @return underlying {@link ByteBuffer} for frame
     */
    public ByteBuffer getByteBuffer() {
        return directBuffer.byteBuffer();
    }

    /**
     * Return frame data as a String
     *
     * @return frame data as {@link System}
     */
    public String getData() {
        return FrameFlyweight.frameData(directBuffer, 0);
    }

    /**
     * Return {@link ByteBuffer} that is a {@link ByteBuffer#slice()} for the frame data
     *
     * @return ByteBuffer containing the data
     */
    public ByteBuffer sliceData()
    {
        return FrameFlyweight.sliceFrameData(directBuffer, 0);
    }

    /**
     * Return frame stream identifier
     *
     * @return frame stream identifier
     */
    public long getStreamId() {
        return FrameFlyweight.streamId(directBuffer);
    }

    /**
     * Return frame {@link FrameType}
     *
     * @return frame type
     */
    public FrameType getType() {
        return FrameFlyweight.frameType(directBuffer);
    }

    /**
     * Return frame version
     *
     * @return frame version
     */
    public int getVersion()
    {
        return FrameFlyweight.version(directBuffer);
    }

    /**
     * Return frame flags
     *
     * @return frame flags
     */
    public int getFlags()
    {
        return FrameFlyweight.flags(directBuffer);
    }

    /**
     * Mutates this Frame to contain the given ByteBuffer
     * 
     * @param byteBuffer to wrap
     */
    public void wrap(final ByteBuffer byteBuffer) {
        this.directBuffer = new UnsafeBuffer(byteBuffer);
    }

    /**
     * Construct a new Frame from the given ByteBuffer
     * 
     * @param byteBuffer to wrap
     * @return new {@link Frame}
     */
    public static Frame from(final ByteBuffer byteBuffer) {
        Frame f = new Frame();
        f.directBuffer = new UnsafeBuffer(byteBuffer);
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
        this.directBuffer = createByteBufferAndEncode(streamId, type, message);
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
        f.directBuffer = createByteBufferAndEncode(streamId, type, message);
        return f;
    }

    private static MutableDirectBuffer createByteBufferAndEncode(long streamId, FrameType type, final String message) {
        // TODO: allocation side effect of how this works currently with the rest of the machinery.
        final MutableDirectBuffer buffer =
            new UnsafeBuffer(ByteBuffer.allocate(FrameFlyweight.computeFrameLength(type, 0, message.length())));

        FrameFlyweight.encode(buffer, streamId, type, null, message);
        return buffer;
    }

    @Override
    public String toString() {
        FrameType type = FrameType.SETUP;
        String payload = "";
        long streamId = -1;

        try
        {
            type = FrameFlyweight.frameType(directBuffer);
            payload = FrameFlyweight.frameData(directBuffer, 0);
            streamId = FrameFlyweight.streamId(directBuffer);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "Frame => ID: " + streamId + " Type: " + type + " Data: " + payload;
    }
}
