package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.IllegalReferenceCountException;
import io.rsocket.Payload;

public class RequestChannelFrameFlyweight {

  private static final RequestFlyweight FLYWEIGHT = new RequestFlyweight(FrameType.REQUEST_CHANNEL);

  private RequestChannelFrameFlyweight() {}

  public static ByteBuf encodeReleasingPayload(
      ByteBufAllocator allocator,
      int streamId,
      boolean complete,
      long initialRequestN,
      Payload payload) {

    // if refCnt exceptions throws here it is safe to do no-op
    boolean hasMetadata = payload.hasMetadata();
    // if refCnt exceptions throws here it is safe to do no-op still
    final ByteBuf metadata = hasMetadata ? payload.metadata().retain() : null;
    final ByteBuf data;
    // retaining data safely. May throw either NPE or RefCntE
    try {
      data = payload.data().retain();
    } catch (IllegalReferenceCountException | NullPointerException e) {
      if (hasMetadata) {
        metadata.release();
      }
      throw e;
    }
    // releasing payload safely since it can be already released wheres we have to release retained
    // data and metadata as well
    try {
      payload.release();
    } catch (IllegalReferenceCountException e) {
      data.release();
      if (hasMetadata) {
        metadata.release();
      }
      throw e;
    }

    return encode(allocator, streamId, false, complete, initialRequestN, metadata, data);
  }

  public static ByteBuf encode(
      ByteBufAllocator allocator,
      int streamId,
      boolean fragmentFollows,
      boolean complete,
      long initialRequestN,
      ByteBuf metadata,
      ByteBuf data) {

    if (initialRequestN < 1) {
      throw new IllegalArgumentException("request n is less than 1");
    }

    int reqN = initialRequestN > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) initialRequestN;

    return FLYWEIGHT.encode(
        allocator, streamId, fragmentFollows, complete, false, reqN, metadata, data);
  }

  public static ByteBuf data(ByteBuf byteBuf) {
    return FLYWEIGHT.dataWithRequestN(byteBuf);
  }

  public static ByteBuf metadata(ByteBuf byteBuf) {
    return FLYWEIGHT.metadataWithRequestN(byteBuf);
  }

  public static long initialRequestN(ByteBuf byteBuf) {
    int requestN = FLYWEIGHT.initialRequestN(byteBuf);
    return requestN == Integer.MAX_VALUE ? Long.MAX_VALUE : requestN;
  }
}
