package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class RequestChannelFrameFlyweight {

  private static final RequestFlyweight FLYWEIGHT = new RequestFlyweight(FrameType.REQUEST_CHANNEL);

  private RequestChannelFrameFlyweight() {}

  public static ByteBuf encode(
      ByteBufAllocator allocator,
      int streamId,
      boolean fragmentFollows,
      boolean complete,
      int requestN,
      ByteBuf metadata,
      ByteBuf data) {

    int i = requestN;
    if (requestN > Integer.MAX_VALUE) {
      i = Integer.MAX_VALUE;
    }

    return FLYWEIGHT.encode(
        allocator, streamId, fragmentFollows, complete, false, i, metadata, data);
  }

  public static ByteBuf data(ByteBuf byteBuf) {
    return FLYWEIGHT.dataWithRequestN(byteBuf);
  }

  public static ByteBuf metadata(ByteBuf byteBuf) {
    return FLYWEIGHT.metadataWithRequestN(byteBuf);
  }

  public static int initialRequestN(ByteBuf byteBuf) {
    return FLYWEIGHT.initialRequestN(byteBuf);
  }
}
