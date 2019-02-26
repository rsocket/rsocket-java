package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Payload;

public class RequestFireAndForgetFrameFlyweight {

  private static final RequestFlyweight FLYWEIGHT = new RequestFlyweight(FrameType.REQUEST_FNF);

  private RequestFireAndForgetFrameFlyweight() {}

  public static ByteBuf encode(
      ByteBufAllocator allocator, int streamId, boolean fragmentFollows, Payload payload) {

    return FLYWEIGHT.encode(
        allocator, streamId, fragmentFollows, payload.metadata(), payload.data());
  }

  public static ByteBuf encode(
      ByteBufAllocator allocator,
      int streamId,
      boolean fragmentFollows,
      ByteBuf metadata,
      ByteBuf data) {

    return FLYWEIGHT.encode(allocator, streamId, fragmentFollows, metadata, data);
  }

  public static ByteBuf data(ByteBuf byteBuf) {
    return FLYWEIGHT.data(byteBuf);
  }

  public static ByteBuf metadata(ByteBuf byteBuf) {
    return FLYWEIGHT.metadata(byteBuf);
  }
}
