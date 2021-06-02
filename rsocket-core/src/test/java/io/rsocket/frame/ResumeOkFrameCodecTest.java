package io.rsocket.frame;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.jupiter.api.Test;

public class ResumeOkFrameCodecTest {

  @Test
  public void testEncoding() {
    ByteBuf byteBuf = ResumeOkFrameCodec.encode(ByteBufAllocator.DEFAULT, 42);
    assertThat(ResumeOkFrameCodec.lastReceivedClientPos(byteBuf)).isEqualTo(42);
    byteBuf.release();
  }
}
