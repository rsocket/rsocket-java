package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExtensionFrameFlyweightTest {

  @Test
  void extensionDataMetadata() {
    ByteBuf metadata = bytebuf("md");
    ByteBuf data = bytebuf("d");
    int extendedType = 1;

    ByteBuf extension =
        ExtensionFrameFlyweight.encode(ByteBufAllocator.DEFAULT, 1, extendedType, metadata, data);

    Assertions.assertTrue(FrameHeaderFlyweight.hasMetadata(extension));
    Assertions.assertEquals(extendedType, ExtensionFrameFlyweight.extendedType(extension));
    Assertions.assertEquals(metadata, ExtensionFrameFlyweight.metadata(extension));
    Assertions.assertEquals(data, ExtensionFrameFlyweight.data(extension));
    extension.release();
  }

  @Test
  void extensionData() {
    ByteBuf data = bytebuf("d");
    int extendedType = 1;

    ByteBuf extension =
        ExtensionFrameFlyweight.encode(ByteBufAllocator.DEFAULT, 1, extendedType, null, data);

    Assertions.assertFalse(FrameHeaderFlyweight.hasMetadata(extension));
    Assertions.assertEquals(extendedType, ExtensionFrameFlyweight.extendedType(extension));
    Assertions.assertEquals(0, ExtensionFrameFlyweight.metadata(extension).readableBytes());
    Assertions.assertEquals(data, ExtensionFrameFlyweight.data(extension));
    extension.release();
  }

  @Test
  void extensionMetadata() {
    ByteBuf metadata = bytebuf("md");
    int extendedType = 1;

    ByteBuf extension =
        ExtensionFrameFlyweight.encode(
            ByteBufAllocator.DEFAULT, 1, extendedType, metadata, Unpooled.EMPTY_BUFFER);

    Assertions.assertTrue(FrameHeaderFlyweight.hasMetadata(extension));
    Assertions.assertEquals(extendedType, ExtensionFrameFlyweight.extendedType(extension));
    Assertions.assertEquals(metadata, ExtensionFrameFlyweight.metadata(extension));
    Assertions.assertEquals(0, ExtensionFrameFlyweight.data(extension).readableBytes());
    extension.release();
  }

  private static ByteBuf bytebuf(String str) {
    return Unpooled.copiedBuffer(str, StandardCharsets.UTF_8);
  }
}
