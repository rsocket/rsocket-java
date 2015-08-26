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

import io.reactivesocket.internal.*;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static java.lang.System.getProperty;

/**
 * Represents a Frame sent over a {@link DuplexConnection}.
 * <p>
 * This provides encoding, decoding and field accessors.
 */
public class Frame implements Payload
{
    public static final ByteBuffer NULL_BYTEBUFFER = FrameHeaderFlyweight.NULL_BYTEBUFFER;

    private static final String FRAME_POOLER_CLASS_NAME =
        getProperty("io.reactivesocket.FramePool", "io.reactivesocket.internal.UnpooledFrame");
    private static final FramePool POOL;

    static
    {
        FramePool tmpPool;

        try
        {
            tmpPool = (FramePool)Class.forName(FRAME_POOLER_CLASS_NAME).newInstance();
        }
        catch (final Exception ex)
        {
            tmpPool = new UnpooledFrame();
        }

        POOL = tmpPool;
    }

    // not final so we can reuse this object
    private MutableDirectBuffer directBuffer;

    private Frame(final MutableDirectBuffer directBuffer)
    {
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
    public ByteBuffer getData()
    {
        return FrameHeaderFlyweight.sliceFrameData(directBuffer, 0, 0);
    }

    /**
     * Return {@link ByteBuffer} that is a {@link ByteBuffer#slice()} for the frame metadata
     *
     * If no metadata is present, the ByteBuffer will have 0 capacity.
     *
     * @return ByteBuffer containing the data
     */
    public ByteBuffer getMetadata()
    {
        return FrameHeaderFlyweight.sliceFrameMetadata(directBuffer, 0, 0);
    }

    /**
     * Return frame stream identifier
     *
     * @return frame stream identifier
     */
    public long getStreamId() {
        return FrameHeaderFlyweight.streamId(directBuffer, 0);
    }

    /**
     * Return frame {@link FrameType}
     *
     * @return frame type
     */
    public FrameType getType() {
        return FrameHeaderFlyweight.frameType(directBuffer, 0);
    }

    /**
     * Return frame version
     *
     * @return frame version
     */
    public int getVersion()
    {
        return FrameHeaderFlyweight.version(directBuffer, 0);
    }

    /**
     * Mutates this Frame to contain the given ByteBuffer
     * 
     * @param byteBuffer to wrap
     */
    public void wrap(final ByteBuffer byteBuffer)
    {
        wrap(POOL.acquireMutableDirectBuffer(byteBuffer));
    }

    /**
     * Mutates this Frame to contain the given MutableDirectBuffer
     *
     * @param directBuffer to wrap
     */
    public void wrap(final MutableDirectBuffer directBuffer)
    {
        this.directBuffer = directBuffer;
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
     * Construct a new Frame from the given {@link MutableDirectBuffer}
     *
     * NOTE: always allocates. Used for pooling.
     *
     * @param directBuffer to wrap
     * @return new {@link Frame}
     */
    public static Frame allocate(final MutableDirectBuffer directBuffer)
    {
        return new Frame(directBuffer);
    }

    /**
     * Release frame for re-use.
     */
    public void release()
    {
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
    public void wrap(final long streamId, final FrameType type, final ByteBuffer data)
    {
        POOL.release(this.directBuffer);

        this.directBuffer =
            POOL.acquireMutableDirectBuffer(FrameHeaderFlyweight.computeFrameHeaderLength(type, 0, data.capacity()));

        FrameHeaderFlyweight.encode(this.directBuffer, 0, streamId, type, NULL_BYTEBUFFER, data);
    }

    public static Frame from(long streamId, FrameType type, ByteBuffer data, ByteBuffer metadata)
    {
        final Frame frame =
            POOL.acquireFrame(FrameHeaderFlyweight.computeFrameHeaderLength(type, metadata.capacity(), data.capacity()));

        FrameHeaderFlyweight.encode(frame.directBuffer, 0, streamId, type, metadata, data);
        return frame;
    }

    public static Frame from(long streamId, FrameType type, ByteBuffer data)
    {
        return from(streamId, type, data, NULL_BYTEBUFFER);
    }

    public static Frame from(long streamId, FrameType type, Payload payload)
    {
    	final ByteBuffer d = payload.getData() != null ? payload.getData() : NULL_BYTEBUFFER;
    	final ByteBuffer md = payload.getMetadata() != null ? payload.getMetadata() : NULL_BYTEBUFFER;

        return from(streamId, type, d, md);
    }

    public static Frame from(long streamId, FrameType type)
    {
        return from(streamId, type, NULL_BYTEBUFFER, NULL_BYTEBUFFER);
    }

    public static Frame fromError(long streamId, final Throwable throwable)
    {
        final byte[] bytes = throwable.getMessage().getBytes();
        final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        
        return from(streamId, FrameType.ERROR, byteBuffer);
    }

    /* TODO:
     *
     * fromRequest(type, id, payload)
     * fromResponse(id, payload, flags)  - does it automatically fragment? (only if auto reassemble)
     * fromError(id, metadata, data) - taken care of by from() overload?
     * fromRequestN(id, n)
     * fromCancel(id, metadata)
     * fromMetadataPush(id, metadata)
     *
     * fromLease(ttl, numberOfRequests, metadata)
     * fromKeepalive(ByteBuffer data)
     */

    public static Frame fromSetup(
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
            POOL.acquireFrame(SetupFrameFlyweight.computeFrameLength(metadataMimeType, dataMimeType, metadata.capacity(), data.capacity()));

        SetupFrameFlyweight.encode(
            frame.directBuffer, 0, flags, keepaliveInterval, maxLifetime, metadataMimeType, dataMimeType, metadata, data);
        return frame;
    }

    public static Frame fromSetupError(int code, String metadata, String data)
    {
        final Frame frame = POOL.acquireFrame(SetupErrorFrameFlyweight.computeFrameLength(data.length(), metadata.length()));

        byte[] bytes;

        bytes = metadata.getBytes(Charset.forName("UTF-8"));
        final ByteBuffer metadataBuffer = ByteBuffer.wrap(bytes);

        bytes = data.getBytes(Charset.forName("UTF-8"));
        final ByteBuffer dataBuffer = ByteBuffer.wrap(bytes);

        SetupErrorFrameFlyweight.encode(frame.directBuffer, 0, code, metadataBuffer, dataBuffer);
        return frame;
    }

    // SETUP specific getters
    public static class Setup
    {
        public static int getFlags(final Frame frame)
        {
            ensureFrameType(FrameType.SETUP, frame);
            final int flags = FrameHeaderFlyweight.flags(frame.directBuffer, 0);

            return flags & (SetupFrameFlyweight.FLAGS_WILL_HONOR_LEASE | SetupFrameFlyweight.FLAGS_STRICT_INTERPRETATION);
        }

        public static int keepaliveInterval(final Frame frame)
        {
            ensureFrameType(FrameType.SETUP, frame);
            return SetupFrameFlyweight.keepaliveInterval(frame.directBuffer, 0);
        }

        public static int maxLifetime(final Frame frame)
        {
            ensureFrameType(FrameType.SETUP, frame);
            return SetupFrameFlyweight.maxLifetime(frame.directBuffer, 0);
        }

        public static String metadataMimeType(final Frame frame)
        {
            ensureFrameType(FrameType.SETUP, frame);
            return SetupFrameFlyweight.metadataMimeType(frame.directBuffer, 0);
        }

        public static String dataMimeType(final Frame frame)
        {
            ensureFrameType(FrameType.SETUP, frame);
            return SetupFrameFlyweight.dataMimeType(frame.directBuffer, 0);
        }
    }

    public static class SetupError
    {
        public static int errorCode(final Frame frame)
        {
            ensureFrameType(FrameType.SETUP_ERROR, frame);
            return SetupErrorFrameFlyweight.errorCode(frame.directBuffer, 0);
        }
    }

    public static void ensureFrameType(final FrameType frameType, final Frame frame)
    {
        final FrameType typeInFrame = frame.getType();

        if (typeInFrame != frameType)
        {
            throw new AssertionError("expected " + frameType + ", but saw" + typeInFrame);
        }
    }

    @Override
    public String toString() {
        FrameType type = FrameType.UNDEFINED;
        StringBuilder payload = new StringBuilder();
        long streamId = -1;

        try
        {
            type = FrameHeaderFlyweight.frameType(directBuffer, 0);
            ByteBuffer byteBuffer;
            byte[] bytes;

            byteBuffer = FrameHeaderFlyweight.sliceFrameMetadata(directBuffer, 0, 0);
            if (0 < byteBuffer.capacity())
            {
                bytes = new byte[byteBuffer.capacity()];
                byteBuffer.get(bytes);
                payload.append(String.format("metadata: \"%s\" ", new String(bytes, Charset.forName("UTF-8"))));
            }

            byteBuffer = FrameHeaderFlyweight.sliceFrameData(directBuffer, 0, 0);
            if (0 < byteBuffer.capacity())
            {
                bytes = new byte[byteBuffer.capacity()];
                byteBuffer.get(bytes);
                payload.append(String.format("data: \"%s\"", new String(bytes, Charset.forName("UTF-8"))));
            }

            streamId = FrameHeaderFlyweight.streamId(directBuffer, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "Frame => Stream ID: " + streamId + " Type: " + type + " Payload Data: " + payload;
    }
}
