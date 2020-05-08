package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.IllegalReferenceCountException;
import io.rsocket.Payload;

public class MetadataPushFrameCodec {

  public static ByteBuf encodeReleasingPayload(ByteBufAllocator allocator, Payload payload) {
    if (!payload.hasMetadata()) {
      throw new IllegalStateException(
          "Metadata  push requires to have metadata present" + " in the given Payload");
    }
    final ByteBuf metadata = payload.metadata().retain();
    // releasing payload safely since it can be already released wheres we have to release retained
    // data and metadata as well
    try {
      payload.release();
    } catch (IllegalReferenceCountException e) {
      metadata.release();
      throw e;
    }
    return encode(allocator, metadata);
  }

  public static ByteBuf encode(ByteBufAllocator allocator, ByteBuf metadata) {
    ByteBuf header =
        FrameHeaderCodec.encodeStreamZero(
            allocator, FrameType.METADATA_PUSH, FrameHeaderCodec.FLAGS_M);
    return allocator.compositeBuffer(2).addComponents(true, header, metadata);
  }

  public static ByteBuf metadata(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    int headerSize = FrameHeaderCodec.size();
    int metadataLength = byteBuf.readableBytes() - headerSize;
    byteBuf.skipBytes(headerSize);
    ByteBuf metadata = byteBuf.readSlice(metadataLength);
    byteBuf.resetReaderIndex();
    return metadata;
  }
}
