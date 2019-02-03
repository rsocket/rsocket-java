package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class ExtensionFrameFlyweight {
  private ExtensionFrameFlyweight() {}

  public static ByteBuf encode(
      ByteBufAllocator allocator, int streamId, int extendedType, ByteBuf data) {
    int flags = FrameHeaderFlyweight.FLAGS_I | FrameHeaderFlyweight.FLAGS_M;
    ByteBuf header = FrameHeaderFlyweight.encode(allocator, streamId, FrameType.EXT, flags);
    header.writeInt(extendedType);
    return allocator.compositeBuffer(2).addComponents(true, header, data);
  }

  public static int extendedType(ByteBuf byteBuf) {
    FrameHeaderFlyweight.ensureFrameType(FrameType.EXT, byteBuf);
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(FrameHeaderFlyweight.size());
    int i = byteBuf.readInt();
    byteBuf.resetReaderIndex();
    return i;
  }

  public static ByteBuf data(ByteBuf byteBuf) {
    FrameHeaderFlyweight.ensureFrameType(FrameType.EXT, byteBuf);
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(FrameHeaderFlyweight.size() + Integer.BYTES);
    ByteBuf data = byteBuf.slice();
    byteBuf.resetReaderIndex();
    return data;
  }
}
