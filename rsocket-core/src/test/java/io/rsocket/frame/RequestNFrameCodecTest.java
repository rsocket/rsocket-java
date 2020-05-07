package io.rsocket.frame;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import org.junit.jupiter.api.Test;

class RequestNFrameCodecTest {
  @Test
  void testEncoding() {
    ByteBuf frame = RequestNFrameCodec.encode(ByteBufAllocator.DEFAULT, 1, 5);

    frame = FrameLengthCodec.encode(ByteBufAllocator.DEFAULT, frame.readableBytes(), frame);
    assertEquals("00000a00000001200000000005", ByteBufUtil.hexDump(frame));
    frame.release();
  }
}
