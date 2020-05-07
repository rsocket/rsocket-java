package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExtensionFrameCodecTest {

  @Test
  void extensionDataMetadata() {
    ByteBuf metadata = bytebuf("md");
    ByteBuf data = bytebuf("d");
    int extendedType = 1;

    ByteBuf extension =
        ExtensionFrameCodec.encode(ByteBufAllocator.DEFAULT, 1, extendedType, metadata, data);

    Assertions.assertTrue(FrameHeaderCodec.hasMetadata(extension));
    Assertions.assertEquals(extendedType, ExtensionFrameCodec.extendedType(extension));
    Assertions.assertEquals(metadata, ExtensionFrameCodec.metadata(extension));
    Assertions.assertEquals(data, ExtensionFrameCodec.data(extension));
    extension.release();
  }

  @Test
  void extensionData() {
    ByteBuf data = bytebuf("d");
    int extendedType = 1;

    ByteBuf extension =
        ExtensionFrameCodec.encode(ByteBufAllocator.DEFAULT, 1, extendedType, null, data);

    Assertions.assertFalse(FrameHeaderCodec.hasMetadata(extension));
    Assertions.assertEquals(extendedType, ExtensionFrameCodec.extendedType(extension));
    Assertions.assertNull(ExtensionFrameCodec.metadata(extension));
    Assertions.assertEquals(data, ExtensionFrameCodec.data(extension));
    extension.release();
  }

  @Test
  void extensionMetadata() {
    ByteBuf metadata = bytebuf("md");
    int extendedType = 1;

    ByteBuf extension =
        ExtensionFrameCodec.encode(
            ByteBufAllocator.DEFAULT, 1, extendedType, metadata, Unpooled.EMPTY_BUFFER);

    Assertions.assertTrue(FrameHeaderCodec.hasMetadata(extension));
    Assertions.assertEquals(extendedType, ExtensionFrameCodec.extendedType(extension));
    Assertions.assertEquals(metadata, ExtensionFrameCodec.metadata(extension));
    Assertions.assertEquals(0, ExtensionFrameCodec.data(extension).readableBytes());
    extension.release();
  }

  private static ByteBuf bytebuf(String str) {
    return Unpooled.copiedBuffer(str, StandardCharsets.UTF_8);
  }
}
