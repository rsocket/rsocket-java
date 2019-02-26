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

  public static ByteBuf encode(
      ByteBufAllocator allocator,
      int streamId,
      boolean fragmentFollows,
      boolean complete,
      boolean next,
      Payload payload) {
    return FLYWEIGHT.encode(
        allocator,
        streamId,
        fragmentFollows,
        complete,
        next,
        0,
        payload.hasMetadata() ? payload.metadata().retain() : null,
        payload.data().retain());
  }

  public static ByteBuf encodeNextComplete(
      ByteBufAllocator allocator, int streamId, Payload payload) {
    return FLYWEIGHT.encode(
        allocator,
        streamId,
        false,
        true,
        true,
        0,
        payload.hasMetadata() ? payload.metadata().retain() : null,
        payload.data().retain());
  }

  public static ByteBuf encodeNext(ByteBufAllocator allocator, int streamId, Payload payload) {
    return FLYWEIGHT.encode(
        allocator,
        streamId,
        false,
        false,
        true,
        0,
        payload.hasMetadata() ? payload.metadata().retain() : null,
        payload.data().retain());
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
