package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import javax.annotation.Nullable;

public class LeaseFlyweight {

  public static ByteBuf encode(
      final ByteBufAllocator allocator,
      final int ttl,
      final int numRequests,
      @Nullable final ByteBuf metadata) {

    int flags = 0;

    if (metadata != null) {
      flags |= FrameHeaderFlyweight.FLAGS_M;
    }

    ByteBuf header =
        FrameHeaderFlyweight.encodeStreamZero(allocator, FrameType.LEASE, flags)
            .writeInt(ttl)
            .writeInt(numRequests);

    if (metadata == null) {
      return header;
    } else {
      return DataAndMetadataFlyweight.encodeOnlyMetadata(allocator, header, metadata);
    }
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
    // Ttl
    byteBuf.skipBytes(FrameHeaderFlyweight.size() + Integer.BYTES);
    int numRequests = byteBuf.readInt();
    byteBuf.resetReaderIndex();
    return numRequests;
  }

  public static ByteBuf metadata(final ByteBuf byteBuf) {
    FrameHeaderFlyweight.ensureFrameType(FrameType.LEASE, byteBuf);
    if (FrameHeaderFlyweight.hasMetadata(byteBuf)) {
      byteBuf.markReaderIndex();
      // Ttl + Num of requests
      byteBuf.skipBytes(FrameHeaderFlyweight.size() + Integer.BYTES * 2);
      ByteBuf metadata = byteBuf.slice();
      byteBuf.resetReaderIndex();
      return metadata;
    } else {
      return Unpooled.EMPTY_BUFFER;
    }
  }
}
