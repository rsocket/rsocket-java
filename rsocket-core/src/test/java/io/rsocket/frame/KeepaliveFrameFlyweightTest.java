package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import static org.junit.Assert.*;

public class KeepaliveFrameFlyweightTest {
    private final ByteBuf byteBuf = Unpooled.buffer(1024);

    @Test
    public void canReadData() {
        ByteBuf data = Unpooled.wrappedBuffer(new byte[]{5, 4, 3});
        int length = KeepaliveFrameFlyweight.encode(byteBuf, KeepaliveFrameFlyweight.FLAGS_KEEPALIVE_R, data);
        data.resetReaderIndex();

        assertEquals(KeepaliveFrameFlyweight.FLAGS_KEEPALIVE_R, FrameHeaderFlyweight.flags(byteBuf) & KeepaliveFrameFlyweight.FLAGS_KEEPALIVE_R);
        assertEquals(data, FrameHeaderFlyweight.sliceFrameData(byteBuf));
    }
}