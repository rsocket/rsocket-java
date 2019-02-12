package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class KeepAliveFrameFlyweight {
  /**
   * (R)espond: Set by the sender of the KEEPALIVE, to which the responder MUST reply with a
   * KEEPALIVE without the R flag set
   */
  public static final int FLAGS_KEEPALIVE_R = 0b00_1000_0000;

  public static final long LAST_POSITION_MASK = 0x8000000000000000L;

  private KeepAliveFrameFlyweight() {}

  public static ByteBuf encode(
      final ByteBufAllocator allocator,
      final boolean respond,
      final long lastPosition,
      final ByteBuf data) {
    final int flags = respond ? FLAGS_KEEPALIVE_R : 0;
    ByteBuf header = FrameHeaderFlyweight.encodeStreamZero(allocator, FrameType.KEEPALIVE, flags);

    long lp = 0;
    if (lastPosition > 0) {
      lp |= lastPosition;
    }

    header.writeLong(lp);

    return DataAndMetadataFlyweight.encodeOnlyData(allocator, header, data);
  }

  public static boolean respondFlag(ByteBuf byteBuf) {
    FrameHeaderFlyweight.ensureFrameType(FrameType.KEEPALIVE, byteBuf);
    int flags = FrameHeaderFlyweight.flags(byteBuf);
    return (flags & FLAGS_KEEPALIVE_R) == FLAGS_KEEPALIVE_R;
  }

  public static long lastPosition(ByteBuf byteBuf) {
    FrameHeaderFlyweight.ensureFrameType(FrameType.KEEPALIVE, byteBuf);
    byteBuf.markReaderIndex();
    long l = byteBuf.skipBytes(FrameHeaderFlyweight.size()).readLong();
    byteBuf.resetReaderIndex();
    return l;
  }

  public static ByteBuf data(ByteBuf byteBuf) {
    FrameHeaderFlyweight.ensureFrameType(FrameType.KEEPALIVE, byteBuf);
    byteBuf.markReaderIndex();
    ByteBuf slice = byteBuf.skipBytes(FrameHeaderFlyweight.size() + Long.BYTES).slice();
    byteBuf.resetReaderIndex();
    return slice;
  }
}
