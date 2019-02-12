package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class RequestNFrameFlyweight {
  private RequestNFrameFlyweight() {}

  public static ByteBuf encode(
      final ByteBufAllocator allocator, final int streamId, long requestN) {
    int reqN = requestN > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) requestN;
    return encode(allocator, streamId, reqN);
  }

  public static ByteBuf encode(final ByteBufAllocator allocator, final int streamId, int requestN) {
    ByteBuf header = FrameHeaderFlyweight.encode(allocator, streamId, FrameType.REQUEST_N, 0);

    if (requestN < 1) {
      throw new IllegalArgumentException("request n is less than 1");
    }

    return header.writeInt(requestN);
  }

  public static int requestN(ByteBuf byteBuf) {
    FrameHeaderFlyweight.ensureFrameType(FrameType.REQUEST_N, byteBuf);
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(FrameHeaderFlyweight.size());
    int i = byteBuf.readInt();
    byteBuf.resetReaderIndex();
    return i;
  }
}
