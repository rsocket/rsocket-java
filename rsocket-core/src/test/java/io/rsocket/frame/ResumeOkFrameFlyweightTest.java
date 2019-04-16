package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

public class ResumeOkFrameFlyweightTest {

  @Test
  public void testEncoding() {
    ByteBuf byteBuf = ResumeOkFrameFlyweight.encode(ByteBufAllocator.DEFAULT, 42);
    Assert.assertEquals(42, ResumeOkFrameFlyweight.lastReceivedClientPos(byteBuf));
    byteBuf.release();
  }
}
