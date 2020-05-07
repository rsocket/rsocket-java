package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class ResumeOkFrameCodec {

  public static ByteBuf encode(final ByteBufAllocator allocator, long lastReceivedClientPos) {
    ByteBuf byteBuf = FrameHeaderCodec.encodeStreamZero(allocator, FrameType.RESUME_OK, 0);
    byteBuf.writeLong(lastReceivedClientPos);
    return byteBuf;
  }

  public static long lastReceivedClientPos(ByteBuf byteBuf) {
    FrameHeaderCodec.ensureFrameType(FrameType.RESUME_OK, byteBuf);
    byteBuf.markReaderIndex();
    long lastReceivedClientPosition = byteBuf.skipBytes(FrameHeaderCodec.size()).readLong();
    byteBuf.resetReaderIndex();

    return lastReceivedClientPosition;
  }
}
