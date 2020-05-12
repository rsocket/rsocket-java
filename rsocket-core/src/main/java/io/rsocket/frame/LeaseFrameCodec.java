package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.util.annotation.Nullable;

public class LeaseFrameCodec {

  public static ByteBuf encode(
      final ByteBufAllocator allocator,
      final int ttl,
      final int numRequests,
      @Nullable final ByteBuf metadata) {

    final boolean hasMetadata = metadata != null;

    int flags = 0;

    if (hasMetadata) {
      flags |= FrameHeaderCodec.FLAGS_M;
    }

    final ByteBuf header =
        FrameHeaderCodec.encodeStreamZero(allocator, FrameType.LEASE, flags)
            .writeInt(ttl)
            .writeInt(numRequests);

    final boolean addMetadata;
    if (hasMetadata) {
      if (metadata.isReadable()) {
        addMetadata = true;
      } else {
        // even though there is nothing to read, we still have to release here since nobody else
        // going to do soo
        metadata.release();
        addMetadata = false;
      }
    } else {
      // has no metadata means it is null, thus no need to release anything
      addMetadata = false;
    }

    if (addMetadata) {
      return allocator.compositeBuffer(2).addComponents(true, header, metadata);
    } else {
      return header;
    }
  }

  public static int ttl(final ByteBuf byteBuf) {
    FrameHeaderCodec.ensureFrameType(FrameType.LEASE, byteBuf);
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(FrameHeaderCodec.size());
    int ttl = byteBuf.readInt();
    byteBuf.resetReaderIndex();
    return ttl;
  }

  public static int numRequests(final ByteBuf byteBuf) {
    FrameHeaderCodec.ensureFrameType(FrameType.LEASE, byteBuf);
    byteBuf.markReaderIndex();
    // Ttl
    byteBuf.skipBytes(FrameHeaderCodec.size() + Integer.BYTES);
    int numRequests = byteBuf.readInt();
    byteBuf.resetReaderIndex();
    return numRequests;
  }

  @Nullable
  public static ByteBuf metadata(final ByteBuf byteBuf) {
    FrameHeaderCodec.ensureFrameType(FrameType.LEASE, byteBuf);
    if (FrameHeaderCodec.hasMetadata(byteBuf)) {
      byteBuf.markReaderIndex();
      // Ttl + Num of requests
      byteBuf.skipBytes(FrameHeaderCodec.size() + Integer.BYTES * 2);
      ByteBuf metadata = byteBuf.slice();
      byteBuf.resetReaderIndex();
      return metadata;
    } else {
      return null;
    }
  }
}
