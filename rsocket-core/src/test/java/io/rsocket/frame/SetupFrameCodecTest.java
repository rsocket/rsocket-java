package io.rsocket.frame;

import static org.junit.jupiter.api.Assertions.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class SetupFrameCodecTest {
  @Test
  void testEncodingNoResume() {
    ByteBuf metadata = Unpooled.wrappedBuffer(new byte[] {1, 2, 3, 4});
    ByteBuf data = Unpooled.wrappedBuffer(new byte[] {5, 4, 3});
    Payload payload = DefaultPayload.create(data, metadata);
    ByteBuf frame =
        SetupFrameCodec.encode(
            ByteBufAllocator.DEFAULT, false, 5, 500, "metadata_type", "data_type", payload);

    assertEquals(FrameType.SETUP, FrameHeaderCodec.frameType(frame));
    assertFalse(SetupFrameCodec.resumeEnabled(frame));
    assertEquals(0, SetupFrameCodec.resumeToken(frame).readableBytes());
    assertEquals("metadata_type", SetupFrameCodec.metadataMimeType(frame));
    assertEquals("data_type", SetupFrameCodec.dataMimeType(frame));
    assertEquals(metadata, SetupFrameCodec.metadata(frame));
    assertEquals(data, SetupFrameCodec.data(frame));
    assertEquals(SetupFrameCodec.CURRENT_VERSION, SetupFrameCodec.version(frame));
    frame.release();
  }

  @Test
  void testEncodingResume() {
    byte[] tokenBytes = new byte[65000];
    Arrays.fill(tokenBytes, (byte) 1);
    ByteBuf metadata = Unpooled.wrappedBuffer(new byte[] {1, 2, 3, 4});
    ByteBuf data = Unpooled.wrappedBuffer(new byte[] {5, 4, 3});
    Payload payload = DefaultPayload.create(data, metadata);
    ByteBuf token = Unpooled.wrappedBuffer(tokenBytes);
    ByteBuf frame =
        SetupFrameCodec.encode(
            ByteBufAllocator.DEFAULT, true, 5, 500, token, "metadata_type", "data_type", payload);

    assertEquals(FrameType.SETUP, FrameHeaderCodec.frameType(frame));
    assertTrue(SetupFrameCodec.honorLease(frame));
    assertTrue(SetupFrameCodec.resumeEnabled(frame));
    assertEquals(token, SetupFrameCodec.resumeToken(frame));
    assertEquals("metadata_type", SetupFrameCodec.metadataMimeType(frame));
    assertEquals("data_type", SetupFrameCodec.dataMimeType(frame));
    assertEquals(metadata, SetupFrameCodec.metadata(frame));
    assertEquals(data, SetupFrameCodec.data(frame));
    assertEquals(SetupFrameCodec.CURRENT_VERSION, SetupFrameCodec.version(frame));
    frame.release();
  }
}
