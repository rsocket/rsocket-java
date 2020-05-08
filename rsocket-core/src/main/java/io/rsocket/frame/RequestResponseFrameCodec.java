package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Payload;
import reactor.util.annotation.Nullable;

public class RequestResponseFrameCodec {

  private RequestResponseFrameCodec() {}

  public static ByteBuf encodeReleasingPayload(
      ByteBufAllocator allocator, int streamId, Payload payload) {

    return GenericFrameCodec.encodeReleasingPayload(
        allocator, FrameType.REQUEST_RESPONSE, streamId, false, false, payload);
  }

  public static ByteBuf encode(
      ByteBufAllocator allocator,
      int streamId,
      boolean fragmentFollows,
      @Nullable ByteBuf metadata,
      ByteBuf data) {
    return GenericFrameCodec.encode(
        allocator, FrameType.REQUEST_RESPONSE, streamId, fragmentFollows, metadata, data);
  }

  public static ByteBuf data(ByteBuf byteBuf) {
    return GenericFrameCodec.data(byteBuf);
  }

  @Nullable
  public static ByteBuf metadata(ByteBuf byteBuf) {
    return GenericFrameCodec.metadata(byteBuf);
  }
}
