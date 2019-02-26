package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class ResumeOkFrameFlyweight {

  public static ByteBuf encode(final ByteBufAllocator allocator, long lastReceivedClientPos) {
    ByteBuf byteBuf = FrameHeaderFlyweight.encodeStreamZero(allocator, FrameType.RESUME_OK, 0);
    byteBuf.writeLong(lastReceivedClientPos);
    return byteBuf;
  }

  public static long lastReceivedClientPos(ByteBuf byteBuf) {
    FrameHeaderFlyweight.ensureFrameType(FrameType.RESUME_OK, byteBuf);
    byteBuf.markReaderIndex();
    long lastReceivedClientPosition = byteBuf
        .skipBytes(FrameHeaderFlyweight.size())
        .readLong();
    byteBuf.resetReaderIndex();

    return lastReceivedClientPosition;
  }
}
