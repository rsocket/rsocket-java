package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Payload;

public class MetadataPushFrameFlyweight {

  public static ByteBuf encodeReleasingPayload(ByteBufAllocator allocator, Payload payload) {
    final ByteBuf metadata = payload.metadata().retain();
    payload.release();
    return encode(allocator, metadata);
  }

  public static ByteBuf encode(ByteBufAllocator allocator, ByteBuf metadata) {
    ByteBuf header =
        FrameHeaderFlyweight.encodeStreamZero(
            allocator, FrameType.METADATA_PUSH, FrameHeaderFlyweight.FLAGS_M);
    return allocator.compositeBuffer(2).addComponents(true, header, metadata);
  }

  public static ByteBuf metadata(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    int headerSize = FrameHeaderFlyweight.size();
    int metadataLength = byteBuf.readableBytes() - headerSize;
    byteBuf.skipBytes(headerSize);
    ByteBuf metadata = byteBuf.readSlice(metadataLength);
    byteBuf.resetReaderIndex();
    return metadata;
  }
}
