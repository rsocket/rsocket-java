package io.rsocket.frame;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.jupiter.api.Test;

class FrameHeaderFlyweightTest {
  // Taken from spec
  private static final int FRAME_MAX_SIZE = 16_777_215;

  @Test
  void typeAndFlag() {
    FrameType frameType = FrameType.REQUEST_FNF;
    int flags = 0b1110110111;
    ByteBuf header = FrameHeaderFlyweight.encode(ByteBufAllocator.DEFAULT, 0, frameType, flags);

    assertEquals(flags, FrameHeaderFlyweight.flags(header));
    assertEquals(frameType, FrameHeaderFlyweight.frameType(header));
    header.release();
  }

  @Test
  void typeAndFlagTruncated() {
    FrameType frameType = FrameType.SETUP;
    int flags = 0b11110110111; // 1 bit too many
    ByteBuf header = FrameHeaderFlyweight.encode(ByteBufAllocator.DEFAULT, 0, frameType, flags);

    assertNotEquals(flags, FrameHeaderFlyweight.flags(header));
    assertEquals(flags & 0b0000_0011_1111_1111, FrameHeaderFlyweight.flags(header));
    assertEquals(frameType, FrameHeaderFlyweight.frameType(header));
    header.release();
  }
}
