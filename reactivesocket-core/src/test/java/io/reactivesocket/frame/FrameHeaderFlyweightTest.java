package io.reactivesocket.frame;

import io.reactivesocket.FrameType;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.nio.ByteBuffer;

import static io.reactivesocket.frame.FrameHeaderFlyweight.NULL_BYTEBUFFER;
import static org.junit.Assert.*;

public class FrameHeaderFlyweightTest {
    private final UnsafeBuffer directBuffer = new UnsafeBuffer(ByteBuffer.allocate(1024));

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