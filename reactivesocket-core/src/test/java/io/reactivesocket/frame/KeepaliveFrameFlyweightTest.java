package io.reactivesocket.frame;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class KeepaliveFrameFlyweightTest {
    private final UnsafeBuffer directBuffer = new UnsafeBuffer(ByteBuffer.allocate(1024));

    @Test
    public void canReadData() {
        ByteBuffer data = ByteBuffer.wrap(new byte[]{5, 4, 3});
        int length = KeepaliveFrameFlyweight.encode(directBuffer, 0, FrameHeaderFlyweight.FLAGS_KEEPALIVE_R, data);
        data.rewind();

        assertEquals(FrameHeaderFlyweight.FLAGS_KEEPALIVE_R, FrameHeaderFlyweight.flags(directBuffer, 0) & FrameHeaderFlyweight.FLAGS_KEEPALIVE_R);
        assertEquals(data, FrameHeaderFlyweight.sliceFrameData(directBuffer, 0, length));
    }
}