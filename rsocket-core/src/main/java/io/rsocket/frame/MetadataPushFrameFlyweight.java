package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class MetadataPushFrameFlyweight {
  public static ByteBuf encode(ByteBufAllocator allocator, ByteBuf metadata) {
    ByteBuf header =
        FrameHeaderFlyweight.encodeStreamZero(
            allocator, FrameType.METADATA_PUSH, FrameHeaderFlyweight.FLAGS_M);
    return allocator.compositeBuffer(2).addComponents(true, header, metadata);
  }
}
