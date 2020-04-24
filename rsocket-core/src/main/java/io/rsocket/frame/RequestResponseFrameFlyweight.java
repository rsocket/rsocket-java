package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Payload;

public class RequestResponseFrameFlyweight {
  private static final RequestFlyweight FLYWEIGHT =
      new RequestFlyweight(FrameType.REQUEST_RESPONSE);

  private RequestResponseFrameFlyweight() {}

  public static ByteBuf encodeReleasingPayload(
      ByteBufAllocator allocator, int streamId, Payload payload) {

    final boolean hasMetadata = payload.hasMetadata();
    final ByteBuf metadata = hasMetadata ? payload.metadata().retain() : null;
    final ByteBuf data = payload.data().retain();

    payload.release();

    return encode(allocator, streamId, false, metadata, data);
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
