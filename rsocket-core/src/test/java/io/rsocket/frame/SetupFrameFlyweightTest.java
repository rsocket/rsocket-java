package io.rsocket.frame;

import static org.junit.jupiter.api.Assertions.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class SetupFrameFlyweightTest {
  @Test
  void testEncodingNoResume() {
    ByteBuf metadata = Unpooled.wrappedBuffer(new byte[] {1, 2, 3, 4});
    ByteBuf data = Unpooled.wrappedBuffer(new byte[] {5, 4, 3});
    ByteBuf frame =
        SetupFrameFlyweight.encode(
            ByteBufAllocator.DEFAULT, false, 5, 500, "metadata_type", "data_type", metadata, data);

    assertEquals(FrameType.SETUP, FrameHeaderFlyweight.frameType(frame));
    assertFalse(SetupFrameFlyweight.resumeEnabled(frame));
    assertNull(SetupFrameFlyweight.resumeToken(frame));
    assertEquals("metadata_type", SetupFrameFlyweight.metadataMimeType(frame));
    assertEquals("data_type", SetupFrameFlyweight.dataMimeType(frame));
    assertEquals(metadata, SetupFrameFlyweight.metadata(frame));
    assertEquals(data, SetupFrameFlyweight.data(frame));
    assertEquals(SetupFrameFlyweight.CURRENT_VERSION, SetupFrameFlyweight.version(frame));
    frame.release();
  }

  @Test
  void testEncodingResume() {
    byte[] tokenBytes = new byte[65000];
    Arrays.fill(tokenBytes, (byte) 1);
    ByteBuf metadata = Unpooled.wrappedBuffer(new byte[] {1, 2, 3, 4});
    ByteBuf data = Unpooled.wrappedBuffer(new byte[] {5, 4, 3});
    ByteBuf token = Unpooled.wrappedBuffer(tokenBytes);
    ByteBuf frame =
        SetupFrameFlyweight.encode(
            ByteBufAllocator.DEFAULT,
            true,
            5,
            500,
            token,
            "metadata_type",
            "data_type",
            metadata,
            data);

    assertEquals(FrameType.SETUP, FrameHeaderFlyweight.frameType(frame));
    assertTrue(SetupFrameFlyweight.honorLease(frame));
    assertTrue(SetupFrameFlyweight.resumeEnabled(frame));
    assertEquals(token, SetupFrameFlyweight.resumeToken(frame));
    assertEquals("metadata_type", SetupFrameFlyweight.metadataMimeType(frame));
    assertEquals("data_type", SetupFrameFlyweight.dataMimeType(frame));
    assertEquals(metadata, SetupFrameFlyweight.metadata(frame));
    assertEquals(data, SetupFrameFlyweight.data(frame));
    assertEquals(SetupFrameFlyweight.CURRENT_VERSION, SetupFrameFlyweight.version(frame));
    frame.release();
  }
}
