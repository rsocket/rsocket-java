package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Payload;
import reactor.util.annotation.Nullable;

public class RequestStreamFrameCodec {

  private RequestStreamFrameCodec() {}

  public static ByteBuf encodeReleasingPayload(
      ByteBufAllocator allocator, int streamId, long initialRequestN, Payload payload) {

    if (initialRequestN < 1) {
      throw new IllegalArgumentException("request n is less than 1");
    }

    int reqN = initialRequestN > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) initialRequestN;

    return GenericFrameCodec.encodeReleasingPayload(
        allocator, FrameType.REQUEST_STREAM, streamId, false, false, reqN, payload);
  }

  public static ByteBuf encode(
      ByteBufAllocator allocator,
      int streamId,
      boolean fragmentFollows,
      long initialRequestN,
      @Nullable ByteBuf metadata,
      ByteBuf data) {

    if (initialRequestN < 1) {
      throw new IllegalArgumentException("request n is less than 1");
    }

    int reqN = initialRequestN > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) initialRequestN;

    return GenericFrameCodec.encode(
        allocator,
        FrameType.REQUEST_STREAM,
        streamId,
        fragmentFollows,
        false,
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
