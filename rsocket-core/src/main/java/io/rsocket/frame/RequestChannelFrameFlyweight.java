package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Payload;

public class RequestChannelFrameFlyweight {

  private static final RequestFlyweight FLYWEIGHT = new RequestFlyweight(FrameType.REQUEST_CHANNEL);

  private RequestChannelFrameFlyweight() {}

  public static ByteBuf encode(
      ByteBufAllocator allocator,
      int streamId,
      boolean fragmentFollows,
      boolean complete,
      long requestN,
      Payload payload) {

    int reqN = requestN > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) requestN;

    return FLYWEIGHT.encode(
        allocator,
        streamId,
        fragmentFollows,
        complete,
        false,
        reqN,
        payload.metadata(),
        payload.data());
  }

  public static ByteBuf encode(
      ByteBufAllocator allocator,
      int streamId,
      boolean fragmentFollows,
      boolean complete,
      long requestN,
      ByteBuf metadata,
      ByteBuf data) {

    int reqN = requestN > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) requestN;

    return FLYWEIGHT.encode(
        allocator, streamId, fragmentFollows, complete, false, reqN, metadata, data);
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
