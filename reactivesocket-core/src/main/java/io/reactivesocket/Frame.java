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
package io.reactivesocket;

import io.reactivesocket.frame.ErrorFrameFlyweight;
import io.reactivesocket.frame.FrameHeaderFlyweight;
import io.reactivesocket.frame.FramePool;
import io.reactivesocket.frame.LeaseFrameFlyweight;
import io.reactivesocket.frame.RequestFrameFlyweight;
import io.reactivesocket.frame.RequestNFrameFlyweight;
import io.reactivesocket.frame.SetupFrameFlyweight;
import io.reactivesocket.frame.UnpooledFrame;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static java.lang.System.getProperty;

/**
 * Represents a Frame sent over a {@link DuplexConnection}.
 * <p>
 * This provides encoding, decoding and field accessors.
 */
public class Frame implements Payload {

    private static final Logger logger = LoggerFactory.getLogger(Frame.class);

    public static final ByteBuffer NULL_BYTEBUFFER = FrameHeaderFlyweight.NULL_BYTEBUFFER;
    public static final int DATA_MTU = 32 * 1024;
    public static final int METADATA_MTU = 32 * 1024;

    /*
     * ThreadLocal handling in the pool itself. We don't have a per thread pool at this level.
     */
    private static final String FRAME_POOLER_CLASS_NAME =
        getProperty("io.reactivesocket.FramePool", UnpooledFrame.class.getName());
        //getProperty("io.reactivesocket.FramePool", ThreadLocalFramePool.class.getName());
    protected static final FramePool POOL;

    static {
        FramePool tmpPool;

        try {
            logger.info("Creating thread pooled named " + FRAME_POOLER_CLASS_NAME);
            tmpPool = (FramePool)Class.forName(FRAME_POOLER_CLASS_NAME).newInstance();
        }
        catch (final Exception ex) {
            logger.error("Error initializing frame pool.", ex);
            tmpPool = new UnpooledFrame();
        }

        POOL = tmpPool;
    }

    // not final so we can reuse this object
    protected MutableDirectBuffer directBuffer;
    protected int offset = 0;
    protected int length = 0;

    protected Frame(final MutableDirectBuffer directBuffer) {
        this.directBuffer = directBuffer;
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
     * Return {@link ByteBuffer} that is a {@link ByteBuffer#slice()} for the frame data
     *
     * If no data is present, the ByteBuffer will have 0 capacity.
     *
     * @return ByteBuffer containing the data
     */
    public ByteBuffer getData() {
        return FrameHeaderFlyweight.sliceFrameData(directBuffer, offset, 0);
    }

    /**
     * Return {@link ByteBuffer} that is a {@link ByteBuffer#slice()} for the frame metadata
     *
     * If no metadata is present, the ByteBuffer will have 0 capacity.
     *
     * @return ByteBuffer containing the data
     */
    public ByteBuffer getMetadata() {
        return FrameHeaderFlyweight.sliceFrameMetadata(directBuffer, offset, 0);
    }

    /**
     * Return frame stream identifier
     *
     * @return frame stream identifier
     */
    public int getStreamId() {
        return FrameHeaderFlyweight.streamId(directBuffer, offset);
    }

    /**
     * Return frame {@link FrameType}
     *
     * @return frame type
     */
    public FrameType getType() {
        return FrameHeaderFlyweight.frameType(directBuffer, offset);
    }

    /**
     * Return the offset in the buffer of the frame
     *
     * @return offset of frame within the buffer
     */
    public int offset() {
        return offset;
    }

    /**
     * Return the encoded length of a frame or the frame length
     *
     * @return frame length
     */
    public int length() {
        return length;
    }

    /**
     * Return the flags field for the frame
     *
     * @return frame flags field value
     */
    public int flags() {
        return FrameHeaderFlyweight.flags(directBuffer, offset);
    }

    /**
     * Mutates this Frame to contain the given ByteBuffer
     * 
     * @param byteBuffer to wrap
     */
    public void wrap(final ByteBuffer byteBuffer, final int offset) {
        wrap(POOL.acquireMutableDirectBuffer(byteBuffer), offset);
    }

    /**
     * Mutates this Frame to contain the given MutableDirectBuffer
     *
     * @param directBuffer to wrap
     */
    public void wrap(final MutableDirectBuffer directBuffer, final int offset) {
        this.directBuffer = directBuffer;
        this.offset = offset;
    }

    /**
     * Acquire a free Frame backed by given ByteBuffer
     * 
     * @param byteBuffer to wrap
     * @return new {@link Frame}
     */
    public static Frame from(final ByteBuffer byteBuffer) {
        return POOL.acquireFrame(byteBuffer);
    }

    /**
     * Acquire a free Frame and back with the given {@link DirectBuffer} starting at offset for length bytes
     *
     * @param directBuffer to use as backing buffer
     * @param offset of start of frame
     * @param length of frame in bytes
     * @return frame
     */
    public static Frame from(final DirectBuffer directBuffer, final int offset, final int length) {
        final Frame frame = POOL.acquireFrame((MutableDirectBuffer)directBuffer);
        frame.offset = offset;
        frame.length = length;

        return frame;
    }

    /**
     * Construct a new Frame from the given {@link MutableDirectBuffer}
     *
     * NOTE: always allocates. Used for pooling.
     *
     * @param directBuffer to wrap
     * @return new {@link Frame}
     */
    public static Frame allocate(final MutableDirectBuffer directBuffer) {
        return new Frame(directBuffer);
    }

    /**
     * Release frame for re-use.
     */
    public void release() {
        POOL.release(this.directBuffer);
        POOL.release(this);
    }

    /**
     * Mutates this Frame to contain the given parameters.
     *
     * NOTE: acquires a new backing buffer and releases current backing buffer
     *
     * @param streamId to include in frame
     * @param type     to include in frame
     * @param data     to include in frame
     */
    public void wrap(final int streamId, final FrameType type, final ByteBuffer data) {
        POOL.release(this.directBuffer);

        this.directBuffer =
            POOL.acquireMutableDirectBuffer(FrameHeaderFlyweight.computeFrameHeaderLength(type, 0, data.remaining()));

        this.length = FrameHeaderFlyweight.encode(this.directBuffer, offset, streamId, 0, type, NULL_BYTEBUFFER, data);
    }

    /* TODO:
     *
     * fromRequest(type, id, payload)
     * fromKeepalive(ByteBuffer data)
     *
     */

    // SETUP specific getters
    public static class Setup {

        private Setup() {}

        public static Frame from(
            int flags,
            int keepaliveInterval,
            int maxLifetime,
            String metadataMimeType,
            String dataMimeType,
            Payload payload)
        {
            final ByteBuffer metadata = payload.getMetadata();
            final ByteBuffer data = payload.getData();

            final Frame frame =
                POOL.acquireFrame(SetupFrameFlyweight.computeFrameLength(metadataMimeType, dataMimeType, metadata.remaining(), data.remaining()));

            frame.length = SetupFrameFlyweight.encode(
                frame.directBuffer, frame.offset, flags, keepaliveInterval, maxLifetime, metadataMimeType, dataMimeType, metadata, data);
            return frame;
        }

        public static int getFlags(final Frame frame) {
            ensureFrameType(FrameType.SETUP, frame);
            final int flags = FrameHeaderFlyweight.flags(frame.directBuffer, frame.offset);

            return flags & (SetupFrameFlyweight.FLAGS_WILL_HONOR_LEASE | SetupFrameFlyweight.FLAGS_STRICT_INTERPRETATION);
        }

        public static int version(final Frame frame) {
            ensureFrameType(FrameType.SETUP, frame);
            return SetupFrameFlyweight.version(frame.directBuffer, frame.offset);
        }

        public static int keepaliveInterval(final Frame frame) {
            ensureFrameType(FrameType.SETUP, frame);
            return SetupFrameFlyweight.keepaliveInterval(frame.directBuffer, frame.offset);
        }

        public static int maxLifetime(final Frame frame) {
            ensureFrameType(FrameType.SETUP, frame);
            return SetupFrameFlyweight.maxLifetime(frame.directBuffer, frame.offset);
        }

        public static String metadataMimeType(final Frame frame) {
            ensureFrameType(FrameType.SETUP, frame);
            return SetupFrameFlyweight.metadataMimeType(frame.directBuffer, frame.offset);
        }

        public static String dataMimeType(final Frame frame) {
            ensureFrameType(FrameType.SETUP, frame);
            return SetupFrameFlyweight.dataMimeType(frame.directBuffer, frame.offset);
        }
    }

    public static class Error {
        private static final Logger errorLogger = LoggerFactory.getLogger(Error.class);

        private Error() {}

        public static Frame from(
            int streamId,
            final Throwable throwable,
            ByteBuffer metadata,
            ByteBuffer data
        ) {
            final int code = ErrorFrameFlyweight.errorCodeFromException(throwable);
            final Frame frame = POOL.acquireFrame(
                ErrorFrameFlyweight.computeFrameLength(metadata.remaining(), data.remaining()));

            if (errorLogger.isDebugEnabled()) {
                errorLogger.debug("an error occurred, creating error frame", throwable);
            }

            frame.length = ErrorFrameFlyweight.encode(
                frame.directBuffer, frame.offset, streamId, code, metadata, data);
            return frame;
        }

        public static Frame from(
            int streamId,
            final Throwable throwable,
            ByteBuffer metadata
        ) {
            String data = throwable.getMessage() == null ? "" : throwable.getMessage();
            byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
            final ByteBuffer dataBuffer = ByteBuffer.wrap(bytes);

            return from(streamId, throwable, metadata, dataBuffer);
        }

        public static Frame from(
            int streamId,
            final Throwable throwable
        ) {
            return from(streamId, throwable, NULL_BYTEBUFFER);
        }

        public static int errorCode(final Frame frame) {
            ensureFrameType(FrameType.ERROR, frame);
            return ErrorFrameFlyweight.errorCode(frame.directBuffer, frame.offset);
        }
    }

    public static class Lease {
        private Lease() {}

        public static Frame from(int ttl, int numberOfRequests, ByteBuffer metadata) {
            final Frame frame = POOL.acquireFrame(LeaseFrameFlyweight.computeFrameLength(metadata.remaining()));

            frame.length = LeaseFrameFlyweight.encode(frame.directBuffer, frame.offset, ttl, numberOfRequests, metadata);
            return frame;
        }

        public static int ttl(final Frame frame) {
            ensureFrameType(FrameType.LEASE, frame);
            return LeaseFrameFlyweight.ttl(frame.directBuffer, frame.offset);
        }

        public static int numberOfRequests(final Frame frame) {
            ensureFrameType(FrameType.LEASE, frame);
            return LeaseFrameFlyweight.numRequests(frame.directBuffer, frame.offset);
        }
    }

    public static class RequestN {
        private RequestN() {}

        public static Frame from(int streamId, long requestN) {
            int v = requestN > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) requestN;
            return from(streamId, v);
        }

        public static Frame from(int streamId, int requestN) {
            final Frame frame = POOL.acquireFrame(RequestNFrameFlyweight.computeFrameLength());

            frame.length = RequestNFrameFlyweight.encode(frame.directBuffer, frame.offset, streamId, requestN);
            return frame;
        }

        public static int requestN(final Frame frame) {
            ensureFrameType(FrameType.REQUEST_N, frame);
            return RequestNFrameFlyweight.requestN(frame.directBuffer, frame.offset);
        }
    }

    public static class Request {
        private Request() {}

        public static Frame from(int streamId, FrameType type, Payload payload, int initialRequestN) {
            final ByteBuffer d = payload.getData() != null ? payload.getData() : NULL_BYTEBUFFER;
            final ByteBuffer md = payload.getMetadata() != null ? payload.getMetadata() : NULL_BYTEBUFFER;

            final Frame frame = POOL.acquireFrame(RequestFrameFlyweight.computeFrameLength(type, md.remaining(), d.remaining()));

            if (type.hasInitialRequestN()) {
                frame.length = RequestFrameFlyweight.encode(frame.directBuffer, frame.offset, streamId, 0, type, initialRequestN, md, d);
            }
            else {
                frame.length = RequestFrameFlyweight.encode(frame.directBuffer, frame.offset, streamId, 0, type, md, d);
            }

            return frame;
        }

        public static Frame from(int streamId, FrameType type, int flags) {
            final Frame frame = POOL.acquireFrame(RequestFrameFlyweight.computeFrameLength(type, 0, 0));

            frame.length = RequestFrameFlyweight.encode(frame.directBuffer, frame.offset, streamId, flags, type, NULL_BYTEBUFFER, NULL_BYTEBUFFER);
            return frame;
        }

        public static Frame from(int streamId, FrameType type, ByteBuffer metadata, ByteBuffer data, int initialRequestN, int flags) {
            final Frame frame = POOL.acquireFrame(RequestFrameFlyweight.computeFrameLength(type, metadata.remaining(), data.remaining()));

            frame.length = RequestFrameFlyweight.encode(frame.directBuffer, frame.offset, streamId, flags, type, initialRequestN, metadata, data);
            return frame;

        }

        public static int initialRequestN(final Frame frame) {
            final FrameType type = frame.getType();
            int result;

            if (!type.isRequestType()) {
                throw new AssertionError("expected request type, but saw " + type.name());
            }

            switch (frame.getType()) {
                case REQUEST_RESPONSE:
                    result = 1;
                    break;
                case FIRE_AND_FORGET:
                    result = 0;
                    break;
                default:
                    result = RequestFrameFlyweight.initialRequestN(frame.directBuffer, frame.offset);
                    break;
            }

            return result;
        }

        public static boolean isRequestChannelComplete(final Frame frame) {
            ensureFrameType(FrameType.REQUEST_CHANNEL, frame);
            final int flags = FrameHeaderFlyweight.flags(frame.directBuffer, frame.offset);

            return (flags & RequestFrameFlyweight.FLAGS_REQUEST_CHANNEL_C) == RequestFrameFlyweight.FLAGS_REQUEST_CHANNEL_C;
        }
    }

    public static class Response {

        private Response() {}

        public static Frame from(int streamId, FrameType type, Payload payload) {
            final ByteBuffer data = payload.getData() != null ? payload.getData() : NULL_BYTEBUFFER;
            final ByteBuffer metadata = payload.getMetadata() != null ? payload.getMetadata() : NULL_BYTEBUFFER;

            final Frame frame =
                POOL.acquireFrame(FrameHeaderFlyweight.computeFrameHeaderLength(type, metadata.remaining(), data.remaining()));

            frame.length = FrameHeaderFlyweight.encode(frame.directBuffer, frame.offset, streamId, 0, type, metadata, data);
            return frame;
        }

        public static Frame from(int streamId, FrameType type, ByteBuffer metadata, ByteBuffer data, int flags) {
            final Frame frame =
                POOL.acquireFrame(FrameHeaderFlyweight.computeFrameHeaderLength(type, metadata.remaining(), data.remaining()));

            frame.length = FrameHeaderFlyweight.encode(frame.directBuffer, frame.offset, streamId, flags, type, metadata, data);
            return frame;
        }

        public static Frame from(int streamId, FrameType type) {
            final Frame frame =
                POOL.acquireFrame(FrameHeaderFlyweight.computeFrameHeaderLength(type, 0, 0));

            frame.length = FrameHeaderFlyweight.encode(
                frame.directBuffer, frame.offset, streamId, 0, type, Frame.NULL_BYTEBUFFER, Frame.NULL_BYTEBUFFER);
            return frame;
        }
    }

    public static class Cancel {

        private Cancel() {}

        public static Frame from(int streamId) {
            final Frame frame =
                POOL.acquireFrame(FrameHeaderFlyweight.computeFrameHeaderLength(FrameType.CANCEL, 0, 0));

            frame.length = FrameHeaderFlyweight.encode(
                frame.directBuffer, frame.offset, streamId, 0, FrameType.CANCEL, Frame.NULL_BYTEBUFFER, Frame.NULL_BYTEBUFFER);
            return frame;
        }
    }

    public static class Keepalive {

        private Keepalive() {}

        public static Frame from(ByteBuffer data, boolean respond) {
            final Frame frame =
                POOL.acquireFrame(FrameHeaderFlyweight.computeFrameHeaderLength(FrameType.KEEPALIVE, 0, data.remaining()));

            final int flags = respond ? FrameHeaderFlyweight.FLAGS_KEEPALIVE_R : 0;

            frame.length = FrameHeaderFlyweight.encode(
                frame.directBuffer, frame.offset, 0, flags, FrameType.KEEPALIVE, Frame.NULL_BYTEBUFFER, data);

            return frame;
        }

        public static boolean hasRespondFlag(final Frame frame) {
            ensureFrameType(FrameType.KEEPALIVE, frame);
            final int flags = FrameHeaderFlyweight.flags(frame.directBuffer, frame.offset);

            return (flags & FrameHeaderFlyweight.FLAGS_KEEPALIVE_R) == FrameHeaderFlyweight.FLAGS_KEEPALIVE_R;
        }
    }

    public static void ensureFrameType(final FrameType frameType, final Frame frame) {
        final FrameType typeInFrame = frame.getType();

        if (typeInFrame != frameType) {
            throw new AssertionError("expected " + frameType + ", but saw" + typeInFrame);
        }
    }

    @Override
    public String toString() {
        FrameType type = FrameType.UNDEFINED;
        StringBuilder payload = new StringBuilder();
        long streamId = -1;
        String additionalFlags = "";

        try {
            type = FrameHeaderFlyweight.frameType(directBuffer, 0);

            ByteBuffer byteBuffer;
            byte[] bytes;

            byteBuffer = FrameHeaderFlyweight.sliceFrameMetadata(directBuffer, 0, 0);
            if (0 < byteBuffer.remaining()) {
                bytes = new byte[byteBuffer.remaining()];
                byteBuffer.get(bytes);
                payload.append(String.format("metadata: \"%s\" ", new String(bytes, StandardCharsets.UTF_8)));
            }

            byteBuffer = FrameHeaderFlyweight.sliceFrameData(directBuffer, 0, 0);
            if (0 < byteBuffer.remaining()) {
                bytes = new byte[byteBuffer.remaining()];
                byteBuffer.get(bytes);
                payload.append(String.format("data: \"%s\"", new String(bytes, StandardCharsets.UTF_8)));
            }

            streamId = FrameHeaderFlyweight.streamId(directBuffer, 0);

            switch (type) {
            case LEASE:
                additionalFlags = " Permits: " + Lease.numberOfRequests(this) + " TTL: " + Lease.ttl(this);
                break;
            case REQUEST_N:
                additionalFlags = " RequestN: " + RequestN.requestN(this);
                break;
            case KEEPALIVE:
                additionalFlags = " Respond flag: " + Keepalive.hasRespondFlag(this);
                break;
            case REQUEST_STREAM:
            case REQUEST_CHANNEL:
                additionalFlags = " Initial Request N: " + Request.initialRequestN(this);
                break;
            case ERROR:
                additionalFlags = " Error code: " + Error.errorCode(this);
                break;
            case SETUP:
                additionalFlags = " Version: " + Setup.version(this)
                                  + " keep-alive interval: " + Setup.keepaliveInterval(this)
                                  + " max lifetime: " + Setup.maxLifetime(this)
                                  + " metadata mime type: " + Setup.metadataMimeType(this)
                                  + " data mime type: " + Setup.dataMimeType(this);
                break;
            }
        } catch (Exception e) {
            logger.error("Error generating toString, ignored.", e);
        }
        return "Frame[" + offset + "] => Stream ID: " + streamId + " Type: " + type
               + (!additionalFlags.isEmpty() ? additionalFlags : "")
               + " Payload: " + payload;
    }
}
