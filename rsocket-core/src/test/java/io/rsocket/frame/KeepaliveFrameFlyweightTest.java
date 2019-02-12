package io.rsocket.frame;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class KeepaliveFrameFlyweightTest {
  @Test
  void canReadData() {
    ByteBuf data = Unpooled.wrappedBuffer(new byte[] {5, 4, 3});
    ByteBuf frame = KeepAliveFrameFlyweight.encode(ByteBufAllocator.DEFAULT, true, 0, data);
    assertTrue(KeepAliveFrameFlyweight.respondFlag(frame));
    assertEquals(data, KeepAliveFrameFlyweight.data(frame));
    frame.release();
  }

  @Test
  void testEncoding() {
    ByteBuf frame =
        KeepAliveFrameFlyweight.encode(
            ByteBufAllocator.DEFAULT, true, 0, Unpooled.copiedBuffer("d", StandardCharsets.UTF_8));
    frame = FrameLengthFlyweight.encode(ByteBufAllocator.DEFAULT, frame.readableBytes(), frame);
    assertEquals("00000f000000000c80000000000000000064", ByteBufUtil.hexDump(frame));
    frame.release();
  }
}
