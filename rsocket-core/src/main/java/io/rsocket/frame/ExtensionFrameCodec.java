package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.util.annotation.Nullable;

public class ExtensionFrameCodec {
  private ExtensionFrameCodec() {}

  public static ByteBuf encode(
      ByteBufAllocator allocator,
      int streamId,
      int extendedType,
      @Nullable ByteBuf metadata,
      ByteBuf data) {

    final boolean hasMetadata = metadata != null;

    int flags = FrameHeaderCodec.FLAGS_I;

    if (hasMetadata) {
      flags |= FrameHeaderCodec.FLAGS_M;
    }

    final ByteBuf header = FrameHeaderCodec.encode(allocator, streamId, FrameType.EXT, flags);
    header.writeInt(extendedType);

    return FrameBodyCodec.encode(allocator, header, metadata, hasMetadata, data);
  }

  public static int extendedType(ByteBuf byteBuf) {
    FrameHeaderCodec.ensureFrameType(FrameType.EXT, byteBuf);
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(FrameHeaderCodec.size());
    int i = byteBuf.readInt();
    byteBuf.resetReaderIndex();
    return i;
  }

  public static ByteBuf data(ByteBuf byteBuf) {
    FrameHeaderCodec.ensureFrameType(FrameType.EXT, byteBuf);

    boolean hasMetadata = FrameHeaderCodec.hasMetadata(byteBuf);
    byteBuf.markReaderIndex();
    // Extended type
    byteBuf.skipBytes(FrameHeaderCodec.size() + Integer.BYTES);
    ByteBuf data = FrameBodyCodec.dataWithoutMarking(byteBuf, hasMetadata);
    byteBuf.resetReaderIndex();
    return data;
  }

  @Nullable
  public static ByteBuf metadata(ByteBuf byteBuf) {
    FrameHeaderCodec.ensureFrameType(FrameType.EXT, byteBuf);

    boolean hasMetadata = FrameHeaderCodec.hasMetadata(byteBuf);
    if (!hasMetadata) {
      return null;
    }
    byteBuf.markReaderIndex();
    // Extended type
    byteBuf.skipBytes(FrameHeaderCodec.size() + Integer.BYTES);
    ByteBuf metadata = FrameBodyCodec.metadataWithoutMarking(byteBuf);
    byteBuf.resetReaderIndex();
    return metadata;
  }
}
