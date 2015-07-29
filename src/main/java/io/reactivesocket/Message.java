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
 * Represents a Message sent over a {@link DuplexConnection}.
 * <p>
 * This provides encoding, decoding and field accessors.
 */
public class Message {

    // TODO: make thread local to demonstrate idea
    private static final ThreadLocal<FrameHandler> FRAME_HANDLER = ThreadLocal.withInitial(FrameHandler::new);

    private Message() {
    }

    // not final so we can reuse this object
    private ByteBuffer b;
    private int streamId;
    private MessageType type;
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

    public MessageType getMessageType() {
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
    public static Message from(ByteBuffer b) {
        Message f = new Message();
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
    public void wrap(int streamId, MessageType type, String message) {
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
    public static Message from(int streamId, MessageType type, String message) {
        Message f = new Message();
        f.b = getBytes(streamId, type, message);
        f.streamId = streamId;
        f.type = type;
        f.message = message;
        return f;
    }

    private static ByteBuffer getBytes(int messageId, MessageType type, String message) {
        // TODO replace with binary
        /**
         * This is NOT how we want it for real. Just representing the idea for discussion.
         */
//        String s = "[" + type.getMessageId() + "]" + getIdString(messageId) + message;
        // TODO stop allocating ... use flywheels
//        return ByteBuffer.wrap(s.getBytes());

        final FrameHandler frameHandler = FRAME_HANDLER.get();

        // TODO: allocation side effect of how this works currently with the rest of the machinery.
        final ByteBuffer buffer = ByteBuffer.allocate(FrameHandler.frameLength(message.length()));

        frameHandler.encode(buffer, messageId, type, message.getBytes());
        return buffer;
    }

    private static String getIdString(int id) {
        return "[" + id + "]|";
    }

    private void decode() {
        // TODO replace with binary
        /**
         * This is NOT how we want it for real. Just representing the idea for discussion.
         */
//        byte[] copy = new byte[b.limit()];
//        b.get(copy);
//        String data = new String(copy);
//        int separator = data.indexOf('|');
//        String prefix = data.substring(0, separator);
//        this.type = MessageType.from(Integer.parseInt(prefix.substring(1, data.indexOf(']'))));
//        this.streamId = Integer.parseInt(prefix.substring(prefix.lastIndexOf("[") + 1, prefix.length() - 1));
//        this.message = data.substring(separator + 1, data.length());

        final FrameHandler frameHandler = FRAME_HANDLER.get();

        frameHandler.decode(b);
        this.type = frameHandler.messageType();
        this.streamId = (int)frameHandler.streamId();  // TODO: temp cast to only touch as little as possible

        // TODO: temp allocation to touch as little as possible
        final byte[] data = new byte[frameHandler.dataLength()];
        frameHandler.getDataBytes(data);
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
        return "Message => ID: " + streamId + " Type: " + type + " Data: " + message;
    }
}
