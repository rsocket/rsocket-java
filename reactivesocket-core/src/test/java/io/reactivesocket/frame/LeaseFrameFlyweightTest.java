package io.reactivesocket.frame;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class LeaseFrameFlyweightTest {
    private final UnsafeBuffer directBuffer = new UnsafeBuffer(ByteBuffer.allocate(1024));

    @Test
    public void size() {
        ByteBuffer metadata = ByteBuffer.wrap(new byte[]{1, 2, 3, 4});
        int length = LeaseFrameFlyweight.encode(directBuffer, 0, 0, 0, metadata);
        assertEquals(length, 9 + 4 * 2 + 4); // Frame header + ttl + #requests + 4 byte metadata
    }
}