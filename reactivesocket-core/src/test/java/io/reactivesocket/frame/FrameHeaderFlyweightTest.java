package io.reactivesocket.frame;

import io.reactivesocket.FrameType;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.nio.ByteBuffer;

import static io.reactivesocket.frame.FrameHeaderFlyweight.NULL_BYTEBUFFER;
import static org.junit.Assert.*;

public class FrameHeaderFlyweightTest {
    // Taken from spec
    private static final int FRAME_MAX_SIZE = 16_777_215;

    private final UnsafeBuffer directBuffer = new UnsafeBuffer(ByteBuffer.allocate(1024));

    @Test
    public void headerSize() {
        int frameLength = 123456;
        FrameHeaderFlyweight.encodeFrameHeader(directBuffer, 0, frameLength, 0, FrameType.SETUP, 0);
        assertEquals(frameLength, FrameHeaderFlyweight.frameLength(directBuffer, 0, frameLength));
    }

    @Test
    public void headerSizeMax() {
        int frameLength = FRAME_MAX_SIZE;
        FrameHeaderFlyweight.encodeFrameHeader(directBuffer, 0, frameLength, 0, FrameType.SETUP, 0);
        assertEquals(frameLength, FrameHeaderFlyweight.frameLength(directBuffer, 0, frameLength));
    }

    @Test(expected = IllegalArgumentException.class)
    public void headerSizeTooLarge() {
        FrameHeaderFlyweight.encodeFrameHeader(directBuffer, 0, FRAME_MAX_SIZE + 1, 0, FrameType.SETUP, 0);
    }

    @Test
    public void frameLength() {
        int length = FrameHeaderFlyweight.encode(directBuffer, 0, 0, 0, FrameType.SETUP, NULL_BYTEBUFFER, NULL_BYTEBUFFER);
        assertEquals(length, 9); // 72 bits
    }

    @Test
    public void metadataLength() {
        ByteBuffer metadata = ByteBuffer.wrap(new byte[]{1, 2, 3, 4});
        FrameHeaderFlyweight.encode(directBuffer, 0, 0, 0, FrameType.SETUP, metadata, NULL_BYTEBUFFER);
        assertEquals(4, FrameHeaderFlyweight.metadataLength(directBuffer, 0, FrameHeaderFlyweight.FRAME_HEADER_LENGTH));
    }

    @Test
    public void dataLength() {
        ByteBuffer data = ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5});
        int length = FrameHeaderFlyweight.encode(directBuffer, 0, 0, 0, FrameType.SETUP, NULL_BYTEBUFFER, data);
        assertEquals(5, FrameHeaderFlyweight.dataLength(directBuffer, 0, length, FrameHeaderFlyweight.FRAME_HEADER_LENGTH));
    }

    @Test
    public void metadataSlice() {
        ByteBuffer metadata = ByteBuffer.wrap(new byte[]{1, 2, 3, 4});
        FrameHeaderFlyweight.encode(directBuffer, 0, 0, 0, FrameType.REQUEST_RESPONSE, metadata, NULL_BYTEBUFFER);
        metadata.rewind();

        assertEquals(metadata, FrameHeaderFlyweight.sliceFrameMetadata(directBuffer, 0, FrameHeaderFlyweight.FRAME_HEADER_LENGTH));
    }

    @Test
    public void dataSlice() {
        ByteBuffer data = ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5});
        FrameHeaderFlyweight.encode(directBuffer, 0, 0, 0, FrameType.REQUEST_RESPONSE, NULL_BYTEBUFFER, data);
        data.rewind();

        assertEquals(data, FrameHeaderFlyweight.sliceFrameData(directBuffer, 0, FrameHeaderFlyweight.FRAME_HEADER_LENGTH));
    }

    @Test
    public void streamId() {
        int streamId = 1234;
        FrameHeaderFlyweight.encode(directBuffer, 0, streamId, 0, FrameType.SETUP, NULL_BYTEBUFFER, NULL_BYTEBUFFER);
        assertEquals(streamId, FrameHeaderFlyweight.streamId(directBuffer, 0));
    }

    @Test
    public void typeAndFlag() {
        FrameType frameType = FrameType.FIRE_AND_FORGET;
        int flags = 0b1110110111;
        FrameHeaderFlyweight.encode(directBuffer, 0, 0, flags, frameType, NULL_BYTEBUFFER, NULL_BYTEBUFFER);

        assertEquals(flags, FrameHeaderFlyweight.flags(directBuffer, 0));
        assertEquals(frameType, FrameHeaderFlyweight.frameType(directBuffer, 0));
    }

    @Test
    public void typeAndFlagTruncated() {
        FrameType frameType = FrameType.SETUP;
        int flags = 0b11110110111; // 1 bit too many
        FrameHeaderFlyweight.encode(directBuffer, 0, 0, flags, FrameType.SETUP, NULL_BYTEBUFFER, NULL_BYTEBUFFER);

        assertNotEquals(flags, FrameHeaderFlyweight.flags(directBuffer, 0));
        assertEquals(flags & 0b0000_0011_1111_1111, FrameHeaderFlyweight.flags(directBuffer, 0));
        assertEquals(frameType, FrameHeaderFlyweight.frameType(directBuffer, 0));
    }
}