package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Payload;
import reactor.util.annotation.Nullable;

public class RequestChannelFrameCodec {

  private RequestChannelFrameCodec() {}

  public static ByteBuf encodeReleasingPayload(
      ByteBufAllocator allocator,
      int streamId,
      boolean complete,
      long initialRequestN,
      Payload payload) {

    if (initialRequestN < 1) {
      throw new IllegalArgumentException("request n is less than 1");
    }

    int reqN = initialRequestN > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) initialRequestN;

    return GenericFrameCodec.encodeReleasingPayload(
        allocator, FrameType.REQUEST_CHANNEL, streamId, complete, false, reqN, payload);
  }

  public static ByteBuf encode(
      ByteBufAllocator allocator,
      int streamId,
      boolean fragmentFollows,
      boolean complete,
      long initialRequestN,
      @Nullable ByteBuf metadata,
      ByteBuf data) {

    if (initialRequestN < 1) {
      throw new IllegalArgumentException("request n is less than 1");
    }

    int reqN = initialRequestN > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) initialRequestN;

    return GenericFrameCodec.encode(
        allocator,
        FrameType.REQUEST_CHANNEL,
        streamId,
        fragmentFollows,
        complete,
        false,
        reqN,
        metadata,
        data);
  }

  public static ByteBuf data(ByteBuf byteBuf) {
    return GenericFrameCodec.dataWithRequestN(byteBuf);
  }

  @Nullable
  public static ByteBuf metadata(ByteBuf byteBuf) {
    return GenericFrameCodec.metadataWithRequestN(byteBuf);
  }

  public static long initialRequestN(ByteBuf byteBuf) {
    int requestN = GenericFrameCodec.initialRequestN(byteBuf);
    return requestN == Integer.MAX_VALUE ? Long.MAX_VALUE : requestN;
  }
}
