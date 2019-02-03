package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RequestFlyweightTest {
  @Test
  void testEncoding() {
    ByteBuf frame =
        RequestStreamFrameFlyweight.encode(
            ByteBufAllocator.DEFAULT,
            1,
            false,
            1,
            Unpooled.copiedBuffer("md", StandardCharsets.UTF_8),
            Unpooled.copiedBuffer("d", StandardCharsets.UTF_8));

    frame = FrameLengthFlyweight.encode(ByteBufAllocator.DEFAULT, frame.readableBytes(), frame);

    assertEquals("000010000000011900000000010000026d6464", ByteBufUtil.hexDump(frame));
    frame.release();
  }
  
  @Test
  void testEncodingWithEmptyMetadata() {
    ByteBuf frame =
      RequestStreamFrameFlyweight.encode(
        ByteBufAllocator.DEFAULT,
        1,
        false,
        1,
        Unpooled.EMPTY_BUFFER,
        Unpooled.copiedBuffer("d", StandardCharsets.UTF_8));
    
    frame = FrameLengthFlyweight.encode(ByteBufAllocator.DEFAULT, frame.readableBytes(), frame);
    
    assertEquals("00000e0000000119000000000100000064", ByteBufUtil.hexDump(frame));
    frame.release();
  }
  
  @Test
  void testEncodingWithNullMetadata() {
    ByteBuf frame =
      RequestStreamFrameFlyweight.encode(
        ByteBufAllocator.DEFAULT,
        1,
        false,
        1,
        null,
        Unpooled.copiedBuffer("d", StandardCharsets.UTF_8));
    
    frame = FrameLengthFlyweight.encode(ByteBufAllocator.DEFAULT, frame.readableBytes(), frame);
    
    assertEquals("00000b0000000118000000000164", ByteBufUtil.hexDump(frame));
    frame.release();
  }
}
