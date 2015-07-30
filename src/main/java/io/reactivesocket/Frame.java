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
    // TODO: make thread local to demonstrate idea
    private static final ThreadLocal<FrameFlyweight> FRAME_HANDLER = ThreadLocal.withInitial(FrameFlyweight::new);

    private Frame() {
    }

    // not final so we can reuse this object
    private ByteBuffer b;
    private int streamId;
    private FrameType type;
    private String message;

    public ByteBuffer getBytes() {
        return b;
    }

    public String getMessage() {
        if (type == null) {
            decode();
        }
        return message;
    }

    public int getStreamId() {
        if (type == null) {
            decode();
        }
        return streamId;
    }

    public FrameType getMessageType() {
        if (type == null) {
            decode();
        }
        return type;
    }

    /**
     * Mutates this Frame to contain the given ByteBuffer
     * 
     * @param b
     */
    public void wrap(ByteBuffer b) {
        this.streamId = -1;
        this.type = null;
        this.message = null;
        this.b = b;
    }

    /**
     * Construct a new Frame from the given ByteBuffer
     * 
     * @param b
     * @return
     */
    public static Frame from(ByteBuffer b) {
        Frame f = new Frame();
        f.b = b;
        return f;
    }

    /**
     * Mutates this Frame to contain the given message.
     * 
     * @param streamId
     * @param type
     * @param message
     */
    public void wrap(int streamId, FrameType type, String message) {
        this.streamId = streamId;
        this.type = type;
        this.message = message;
        this.b = getBytes(streamId, type, message);
    }

    /**
     * Construct a new Frame with the given message.
     * 
     * @param streamId
     * @param type
     * @param message
     * @return
     */
    public static Frame from(int streamId, FrameType type, String message) {
        Frame f = new Frame();
        f.b = getBytes(streamId, type, message);
        f.streamId = streamId;
        f.type = type;
        f.message = message;
        return f;
    }

    private static ByteBuffer getBytes(int messageId, FrameType type, String message) {
        final FrameFlyweight frameFlyweight = FRAME_HANDLER.get();

        // TODO: allocation side effect of how this works currently with the rest of the machinery.
        final ByteBuffer buffer = ByteBuffer.allocate(FrameFlyweight.frameLength(message.length()));

        frameFlyweight.encode(buffer, messageId, type, message.getBytes());
        return buffer;
    }

    private void decode() {
        final FrameFlyweight frameFlyweight = FRAME_HANDLER.get();

        frameFlyweight.decode(b);
        this.type = frameFlyweight.messageType();
        this.streamId = (int) frameFlyweight.streamId();  // TODO: temp cast to only touch as little as possible

        // TODO: temp allocation to touch as little as possible
        final byte[] data = new byte[frameFlyweight.dataLength()];
        frameFlyweight.getDataBytes(data);
        this.message = new String(data);
    }

    @Override
    public String toString() {
        if (type == null) {
            try {
                decode();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return "Frame => ID: " + streamId + " Type: " + type + " Data: " + message;
    }
}
