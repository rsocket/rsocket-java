package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class CancelFrameCodec {
  private CancelFrameCodec() {}

  public static ByteBuf encode(final ByteBufAllocator allocator, final int streamId) {
    return FrameHeaderCodec.encode(allocator, streamId, FrameType.CANCEL, 0);
  }
}
