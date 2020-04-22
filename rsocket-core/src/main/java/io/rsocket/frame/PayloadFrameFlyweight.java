package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Payload;

public class PayloadFrameFlyweight {
  private static final RequestFlyweight FLYWEIGHT = new RequestFlyweight(FrameType.PAYLOAD);

  private PayloadFrameFlyweight() {}

  public static ByteBuf encode(
      ByteBufAllocator allocator,
      int streamId,
      boolean fragmentFollows,
      boolean complete,
      boolean next,
      ByteBuf metadata,
      ByteBuf data) {
    return FLYWEIGHT.encode(
        allocator, streamId, fragmentFollows, complete, next, 0, metadata, data);
  }

  public static ByteBuf encodeNext(ByteBufAllocator allocator, int streamId, Payload payload) {
    return encode(allocator, streamId, false, payload);
  }

  public static ByteBuf encodeNextComplete(
      ByteBufAllocator allocator, int streamId, Payload payload) {

    return encode(allocator, streamId, true, payload);
  }

  static ByteBuf encode(
      ByteBufAllocator allocator, int streamId, boolean complete, Payload payload) {

    final boolean hasMetadata = payload.hasMetadata();
    final ByteBuf metadata = hasMetadata ? payload.metadata().retain() : null;
    final ByteBuf data = payload.data().retain();

    return FLYWEIGHT.encode(allocator, streamId, false, complete, true, 0, metadata, data);
  }

  public static ByteBuf encodeComplete(ByteBufAllocator allocator, int streamId) {
    return FLYWEIGHT.encode(allocator, streamId, false, true, false, 0, null, null);
  }

  public static ByteBuf data(ByteBuf byteBuf) {
    return FLYWEIGHT.data(byteBuf);
  }

  public static ByteBuf metadata(ByteBuf byteBuf) {
    return FLYWEIGHT.metadata(byteBuf);
  }
}
