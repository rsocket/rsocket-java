package io.rsocket.frame;

import static org.junit.jupiter.api.Assertions.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class GenericFrameCodecTest {
  @Test
  void testEncoding() {
    ByteBuf frame =
        RequestStreamFrameCodec.encode(
            ByteBufAllocator.DEFAULT,
            1,
            false,
            1,
            Unpooled.copiedBuffer("md", StandardCharsets.UTF_8),
            Unpooled.copiedBuffer("d", StandardCharsets.UTF_8));

    frame = FrameLengthCodec.encode(ByteBufAllocator.DEFAULT, frame.readableBytes(), frame);
    // Encoded FrameLength⌍        ⌌ Encoded Headers
    //                   |        |         ⌌ Encoded Request(1)
    //                   |        |         |      ⌌Encoded Metadata Length
    //                   |        |         |      |    ⌌Encoded Metadata
    //                   |        |         |      |    |   ⌌Encoded Data
    //                 __|________|_________|______|____|___|
    //                 ↓    ↓↓          ↓↓      ↓↓    ↓↓  ↓↓↓
    String expected = "000010000000011900000000010000026d6464";
    assertEquals(expected, ByteBufUtil.hexDump(frame));
    frame.release();
  }

  @Test
  void testEncodingWithEmptyMetadata() {
    ByteBuf frame =
        RequestStreamFrameCodec.encode(
            ByteBufAllocator.DEFAULT,
            1,
            false,
            1,
            Unpooled.EMPTY_BUFFER,
            Unpooled.copiedBuffer("d", StandardCharsets.UTF_8));

    frame = FrameLengthCodec.encode(ByteBufAllocator.DEFAULT, frame.readableBytes(), frame);
    // Encoded FrameLength⌍        ⌌ Encoded Headers
    //                   |        |         ⌌ Encoded Request(1)
    //                   |        |         |       ⌌Encoded Metadata Length (0)
    //                   |        |         |       |   ⌌Encoded Data
    //                 __|________|_________|_______|___|
    //                 ↓    ↓↓          ↓↓      ↓↓    ↓↓↓
    String expected = "00000e0000000119000000000100000064";
    assertEquals(expected, ByteBufUtil.hexDump(frame));
    frame.release();
  }

  @Test
  void testEncodingWithNullMetadata() {
    ByteBuf frame =
        RequestStreamFrameCodec.encode(
            ByteBufAllocator.DEFAULT,
            1,
            false,
            1,
            null,
            Unpooled.copiedBuffer("d", StandardCharsets.UTF_8));

    frame = FrameLengthCodec.encode(ByteBufAllocator.DEFAULT, frame.readableBytes(), frame);

    // Encoded FrameLength⌍        ⌌ Encoded Headers
    //                   |        |         ⌌ Encoded Request(1)
    //                   |        |         |     ⌌Encoded Data
    //                 __|________|_________|_____|
    //                 ↓<-> ↓↓   <->    ↓↓ <->  ↓↓↓
    String expected = "00000b0000000118000000000164";
    assertEquals(expected, ByteBufUtil.hexDump(frame));
    frame.release();
  }

  @Test
  void requestResponseDataMetadata() {
    ByteBuf request =
        RequestResponseFrameCodec.encode(
            ByteBufAllocator.DEFAULT,
            1,
            false,
            Unpooled.copiedBuffer("md", StandardCharsets.UTF_8),
            Unpooled.copiedBuffer("d", StandardCharsets.UTF_8));

    String data = RequestResponseFrameCodec.data(request).toString(StandardCharsets.UTF_8);
    String metadata = RequestResponseFrameCodec.metadata(request).toString(StandardCharsets.UTF_8);

    assertTrue(FrameHeaderCodec.hasMetadata(request));
    assertEquals("d", data);
    assertEquals("md", metadata);
    request.release();
  }

  @Test
  void requestResponseData() {
    ByteBuf request =
        RequestResponseFrameCodec.encode(
            ByteBufAllocator.DEFAULT,
            1,
            false,
            null,
            Unpooled.copiedBuffer("d", StandardCharsets.UTF_8));

    String data = RequestResponseFrameCodec.data(request).toString(StandardCharsets.UTF_8);
    ByteBuf metadata = RequestResponseFrameCodec.metadata(request);

    assertFalse(FrameHeaderCodec.hasMetadata(request));
    assertEquals("d", data);
    assertNull(metadata);
    request.release();
  }

  @Test
  void requestResponseMetadata() {
    ByteBuf request =
        RequestResponseFrameCodec.encode(
            ByteBufAllocator.DEFAULT,
            1,
            false,
            Unpooled.copiedBuffer("md", StandardCharsets.UTF_8),
            Unpooled.EMPTY_BUFFER);

    ByteBuf data = RequestResponseFrameCodec.data(request);
    String metadata = RequestResponseFrameCodec.metadata(request).toString(StandardCharsets.UTF_8);

    assertTrue(FrameHeaderCodec.hasMetadata(request));
    assertTrue(data.readableBytes() == 0);
    assertEquals("md", metadata);
    request.release();
  }

  @Test
  void requestStreamDataMetadata() {
    ByteBuf request =
        RequestStreamFrameCodec.encode(
            ByteBufAllocator.DEFAULT,
            1,
            false,
            Integer.MAX_VALUE + 1L,
            Unpooled.copiedBuffer("md", StandardCharsets.UTF_8),
            Unpooled.copiedBuffer("d", StandardCharsets.UTF_8));

    long actualRequest = RequestStreamFrameCodec.initialRequestN(request);
    String data = RequestStreamFrameCodec.data(request).toString(StandardCharsets.UTF_8);
    String metadata = RequestStreamFrameCodec.metadata(request).toString(StandardCharsets.UTF_8);

    assertTrue(FrameHeaderCodec.hasMetadata(request));
    assertEquals(Long.MAX_VALUE, actualRequest);
    assertEquals("md", metadata);
    assertEquals("d", data);
    request.release();
  }

  @Test
  void requestStreamData() {
    ByteBuf request =
        RequestStreamFrameCodec.encode(
            ByteBufAllocator.DEFAULT,
            1,
            false,
            42,
            null,
            Unpooled.copiedBuffer("d", StandardCharsets.UTF_8));

    long actualRequest = RequestStreamFrameCodec.initialRequestN(request);
    String data = RequestStreamFrameCodec.data(request).toString(StandardCharsets.UTF_8);
    ByteBuf metadata = RequestStreamFrameCodec.metadata(request);

    assertFalse(FrameHeaderCodec.hasMetadata(request));
    assertEquals(42L, actualRequest);
    assertNull(metadata);
    assertEquals("d", data);
    request.release();
  }

  @Test
  void requestStreamMetadata() {
    ByteBuf request =
        RequestStreamFrameCodec.encode(
            ByteBufAllocator.DEFAULT,
            1,
            false,
            42,
            Unpooled.copiedBuffer("md", StandardCharsets.UTF_8),
            Unpooled.EMPTY_BUFFER);

    long actualRequest = RequestStreamFrameCodec.initialRequestN(request);
    ByteBuf data = RequestStreamFrameCodec.data(request);
    String metadata = RequestStreamFrameCodec.metadata(request).toString(StandardCharsets.UTF_8);

    assertTrue(FrameHeaderCodec.hasMetadata(request));
    assertEquals(42L, actualRequest);
    assertTrue(data.readableBytes() == 0);
    assertEquals("md", metadata);
    request.release();
  }

  @Test
  void requestFnfDataAndMetadata() {
    ByteBuf request =
        RequestFireAndForgetFrameCodec.encode(
            ByteBufAllocator.DEFAULT,
            1,
            false,
            Unpooled.copiedBuffer("md", StandardCharsets.UTF_8),
            Unpooled.copiedBuffer("d", StandardCharsets.UTF_8));

    String data = RequestFireAndForgetFrameCodec.data(request).toString(StandardCharsets.UTF_8);
    String metadata =
        RequestFireAndForgetFrameCodec.metadata(request).toString(StandardCharsets.UTF_8);

    assertTrue(FrameHeaderCodec.hasMetadata(request));
    assertEquals("d", data);
    assertEquals("md", metadata);
    request.release();
  }

  @Test
  void requestFnfData() {
    ByteBuf request =
        RequestFireAndForgetFrameCodec.encode(
            ByteBufAllocator.DEFAULT,
            1,
            false,
            null,
            Unpooled.copiedBuffer("d", StandardCharsets.UTF_8));

    String data = RequestFireAndForgetFrameCodec.data(request).toString(StandardCharsets.UTF_8);
    ByteBuf metadata = RequestFireAndForgetFrameCodec.metadata(request);

    assertFalse(FrameHeaderCodec.hasMetadata(request));
    assertEquals("d", data);
    assertNull(metadata);
    request.release();
  }

  @Test
  void requestFnfMetadata() {
    ByteBuf request =
        RequestFireAndForgetFrameCodec.encode(
            ByteBufAllocator.DEFAULT,
            1,
            false,
            Unpooled.copiedBuffer("md", StandardCharsets.UTF_8),
            Unpooled.EMPTY_BUFFER);

    ByteBuf data = RequestFireAndForgetFrameCodec.data(request);
    String metadata =
        RequestFireAndForgetFrameCodec.metadata(request).toString(StandardCharsets.UTF_8);

    assertTrue(FrameHeaderCodec.hasMetadata(request));
    assertEquals("md", metadata);
    assertTrue(data.readableBytes() == 0);
    request.release();
  }
}
