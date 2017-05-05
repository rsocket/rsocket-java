package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import static org.junit.Assert.*;

public class LeaseFrameFlyweightTest {
    private final ByteBuf byteBuf = Unpooled.buffer(1024);

    @Test
    public void size() {
        ByteBuf metadata = Unpooled.wrappedBuffer(new byte[]{1, 2, 3, 4});
        int length = LeaseFrameFlyweight.encode(byteBuf, 0, 0, metadata);
        assertEquals(length, 9 + 4 * 2 + 4); // Frame header + ttl + #requests + 4 byte metadata
    }
}