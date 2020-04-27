package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.IllegalReferenceCountException;
import io.rsocket.Payload;

public class RequestFireAndForgetFrameFlyweight {

  private static final RequestFlyweight FLYWEIGHT = new RequestFlyweight(FrameType.REQUEST_FNF);

  private RequestFireAndForgetFrameFlyweight() {}

  public static ByteBuf encodeReleasingPayload(
      ByteBufAllocator allocator, int streamId, Payload payload) {

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

    return FLYWEIGHT.encode(allocator, streamId, false, metadata, data);
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
