package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class ResumeFrameFlyweightTest {
  
  @Test
  void testEncoding() {
    byte[] tokenBytes = new byte[65000];
    Arrays.fill(tokenBytes, (byte) 1);
    ByteBuf byteBuf = ResumeFrameFlyweight.encode(ByteBufAllocator.DEFAULT, tokenBytes, 21, 12);
    Assert.assertEquals(ResumeFrameFlyweight.CURRENT_VERSION, ResumeFrameFlyweight.version(byteBuf));
    Assert.assertArrayEquals(tokenBytes, ResumeFrameFlyweight.token(byteBuf));
    Assert.assertEquals(21, ResumeFrameFlyweight.lastReceivedServerPos(byteBuf));
    Assert.assertEquals(12, ResumeFrameFlyweight.firstAvailableClientPos(byteBuf));
    byteBuf.release();
  }
}
