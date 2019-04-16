package io.rsocket.frame;

import io.netty.buffer.*;
import org.junit.jupiter.api.Test;

class DataAndMetadataFlyweightTest {
  @Test
  void testEncodeData() {
    ByteBuf header = FrameHeaderFlyweight.encode(ByteBufAllocator.DEFAULT, 1, FrameType.PAYLOAD, 0);
    ByteBuf data = ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, "_I'm data_");
    ByteBuf frame = DataAndMetadataFlyweight.encodeOnlyData(ByteBufAllocator.DEFAULT, header, data);
    ByteBuf d = DataAndMetadataFlyweight.data(frame, false);
    String s = ByteBufUtil.prettyHexDump(d);
    System.out.println(s);
  }

  @Test
  void testEncodeMetadata() {
    ByteBuf header = FrameHeaderFlyweight.encode(ByteBufAllocator.DEFAULT, 1, FrameType.PAYLOAD, 0);
    ByteBuf data = ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, "_I'm metadata_");
    ByteBuf frame =
        DataAndMetadataFlyweight.encodeOnlyMetadata(ByteBufAllocator.DEFAULT, header, data);
    ByteBuf d = DataAndMetadataFlyweight.data(frame, false);
    String s = ByteBufUtil.prettyHexDump(d);
    System.out.println(s);
  }

  @Test
  void testEncodeDataAndMetadata() {
    ByteBuf header =
        FrameHeaderFlyweight.encode(ByteBufAllocator.DEFAULT, 1, FrameType.REQUEST_RESPONSE, 0);
    ByteBuf data = ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, "_I'm data_");
    ByteBuf metadata = ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, "_I'm metadata_");
    ByteBuf frame =
        DataAndMetadataFlyweight.encode(ByteBufAllocator.DEFAULT, header, metadata, data);
    ByteBuf m = DataAndMetadataFlyweight.metadata(frame, true);
    String s = ByteBufUtil.prettyHexDump(m);
    System.out.println(s);
    FrameType frameType = FrameHeaderFlyweight.frameType(frame);
    System.out.println(frameType);

    for (int i = 0; i < 10_000_000; i++) {
      ByteBuf d1 = ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, "_I'm data_");
      ByteBuf m1 = ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, "_I'm metadata_");
      ByteBuf h1 =
          FrameHeaderFlyweight.encode(ByteBufAllocator.DEFAULT, 1, FrameType.REQUEST_RESPONSE, 0);
      ByteBuf f1 = DataAndMetadataFlyweight.encode(ByteBufAllocator.DEFAULT, h1, m1, d1);
      f1.release();
    }
  }
}
