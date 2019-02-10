package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SetupFrameFlyweightTest {
  @Test
  void validFrame() {
    ByteBuf metadata = Unpooled.wrappedBuffer(new byte[] {1, 2, 3, 4});
    ByteBuf data = Unpooled.wrappedBuffer(new byte[] {5, 4, 3});
    ByteBuf frame =
        SetupFrameFlyweight.encode(
            ByteBufAllocator.DEFAULT,
            false,
            false,
            5,
            500,
            "metadata_type",
            "data_type",
            metadata,
            data);

    assertEquals(FrameType.SETUP, FrameHeaderFlyweight.frameType(frame));
    assertEquals("metadata_type", SetupFrameFlyweight.metadataMimeType(frame));
    assertEquals("data_type", SetupFrameFlyweight.dataMimeType(frame));
    assertEquals(metadata, SetupFrameFlyweight.metadata(frame));
    assertEquals(data, SetupFrameFlyweight.data(frame));
    assertEquals(SetupFrameFlyweight.CURRENT_VERSION, SetupFrameFlyweight.version(frame));
    frame.release();
  }

  @Test
  void resumeNotSupported() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SetupFrameFlyweight.encode(
                ByteBufAllocator.DEFAULT,
                false,
                true,
                5,
                500,
                "",
                "",
                Unpooled.EMPTY_BUFFER,
                Unpooled.EMPTY_BUFFER));
  }

  @Test
  public void testEncoding() {
    ByteBuf frame =
        SetupFrameFlyweight.encode(
            ByteBufAllocator.DEFAULT,
            false,
            false,
            5000,
            60000,
            "mdmt",
            "dmt",
            Unpooled.copiedBuffer("md", StandardCharsets.UTF_8),
            Unpooled.copiedBuffer("d", StandardCharsets.UTF_8));
    frame = FrameLengthFlyweight.encode(ByteBufAllocator.DEFAULT, frame.readableBytes(), frame);
    assertEquals(
        "00002100000000050000010000000013880000ea60046d646d7403646d740000026d6464",
        ByteBufUtil.hexDump(frame));
    frame.release();
  }
}
