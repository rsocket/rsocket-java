package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class LeaseFlyweight {

  public static ByteBuf encode(
      final ByteBufAllocator allocator,
      final int ttl,
      final int numRequests,
      final ByteBuf metadata) {
    ByteBuf header =
        FrameHeaderFlyweight.encodeStreamZero(allocator, FrameType.LEASE, 0)
            .writeInt(ttl)
            .writeInt(numRequests);

    return DataAndMetadataFlyweight.encodeOnlyMetadata(allocator, header, metadata);
  }

  public static int ttl(final ByteBuf byteBuf) {
    FrameHeaderFlyweight.ensureFrameType(FrameType.LEASE, byteBuf);
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(FrameHeaderFlyweight.size());
    int ttl = byteBuf.readInt();
    byteBuf.resetReaderIndex();
    return ttl;
  }

  public static int numRequests(final ByteBuf byteBuf) {
    FrameHeaderFlyweight.ensureFrameType(FrameType.LEASE, byteBuf);
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(FrameHeaderFlyweight.size() + Integer.BYTES);
    int numRequests = byteBuf.readInt();
    byteBuf.resetReaderIndex();
    return numRequests;
  }

  public static ByteBuf metadata(final ByteBuf byteBuf) {
    FrameHeaderFlyweight.ensureFrameType(FrameType.LEASE, byteBuf);
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(FrameHeaderFlyweight.size() + Integer.BYTES * 2);
    ByteBuf metadata = DataAndMetadataFlyweight.metadataWithoutMarking(byteBuf);
    byteBuf.resetReaderIndex();
    return metadata;
  }
}
