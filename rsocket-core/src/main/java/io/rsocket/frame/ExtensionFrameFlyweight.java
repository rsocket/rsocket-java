package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import javax.annotation.Nullable;

public class ExtensionFrameFlyweight {
  private ExtensionFrameFlyweight() {}

  public static ByteBuf encode(
      ByteBufAllocator allocator, int streamId, int extendedType,
      @Nullable ByteBuf metadata, ByteBuf data) {

    int flags = FrameHeaderFlyweight.FLAGS_I;

    if (metadata != null) {
      flags |= FrameHeaderFlyweight.FLAGS_M;
    }

    ByteBuf header = FrameHeaderFlyweight.encode(allocator, streamId, FrameType.EXT, flags);
    header.writeInt(extendedType);
    if (metadata != null) {
      return DataAndMetadataFlyweight.encode(allocator, header, metadata, data);
    } else {
      return DataAndMetadataFlyweight.encodeOnlyData(allocator, header, data);
    }
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

    boolean hasMetadata = FrameHeaderFlyweight.hasMetadata(byteBuf);
    byteBuf.markReaderIndex();
    //Extended type
    byteBuf.skipBytes(FrameHeaderFlyweight.size() + Integer.BYTES);
    ByteBuf data = DataAndMetadataFlyweight.dataWithoutMarking(byteBuf, hasMetadata);
    byteBuf.resetReaderIndex();
    return data;
  }

  public static ByteBuf metadata(ByteBuf byteBuf) {
    FrameHeaderFlyweight.ensureFrameType(FrameType.EXT, byteBuf);

    boolean hasMetadata = FrameHeaderFlyweight.hasMetadata(byteBuf);
    byteBuf.markReaderIndex();
    //Extended type
    byteBuf.skipBytes(FrameHeaderFlyweight.size() + Integer.BYTES);
    ByteBuf metadata = DataAndMetadataFlyweight.metadataWithoutMarking(byteBuf, hasMetadata);
    byteBuf.resetReaderIndex();
    return metadata;
  }
}
