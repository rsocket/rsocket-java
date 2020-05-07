package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

public class ResumeOkFrameCodecTest {

  @Test
  public void testEncoding() {
    ByteBuf byteBuf = ResumeOkFrameCodec.encode(ByteBufAllocator.DEFAULT, 42);
    Assert.assertEquals(42, ResumeOkFrameCodec.lastReceivedClientPos(byteBuf));
    byteBuf.release();
  }
}
