package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Payload;

public class RequestStreamFrameFlyweight {

  private static final RequestFlyweight FLYWEIGHT = new RequestFlyweight(FrameType.REQUEST_STREAM);

  private RequestStreamFrameFlyweight() {}

  public static ByteBuf encode(
      ByteBufAllocator allocator,
      int streamId,
      boolean fragmentFollows,
      long requestN,
      Payload payload) {
    return encode(
        allocator,
        streamId,
        fragmentFollows,
        requestN,
        payload.sliceMetadata(),
        payload.sliceData());
  }

  public static ByteBuf encode(
      ByteBufAllocator allocator,
      int streamId,
      boolean fragmentFollows,
      int requestN,
      Payload payload) {
    return encode(
        allocator,
        streamId,
        fragmentFollows,
        requestN,
        payload.sliceMetadata(),
        payload.sliceData());
  }

  public static ByteBuf encode(
      ByteBufAllocator allocator,
      int streamId,
      boolean fragmentFollows,
      long requestN,
      ByteBuf metadata,
      ByteBuf data) {
    int reqN = requestN > Integer.MAX_VALUE
        ? Integer.MAX_VALUE
        : (int) requestN;
    return encode(allocator, streamId, fragmentFollows, reqN, metadata, data);
  }

  public static ByteBuf encode(
      ByteBufAllocator allocator,
      int streamId,
      boolean fragmentFollows,
      int requestN,
      ByteBuf metadata,
      ByteBuf data) {
    return FLYWEIGHT.encode(
        allocator, streamId, fragmentFollows, false, false, requestN, metadata, data);
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
