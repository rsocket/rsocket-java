package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class CancelFrameFlyweight {
  private CancelFrameFlyweight() {}

  public static ByteBuf encode(final ByteBufAllocator allocator, final int streamId) {
    return FrameHeaderFlyweight.encode(allocator, streamId, FrameType.CANCEL, 0);
  }
}
