package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Payload;
import reactor.util.annotation.Nullable;

public class PayloadFrameCodec {

  private PayloadFrameCodec() {}

  public static ByteBuf encodeNextReleasingPayload(
      ByteBufAllocator allocator, int streamId, Payload payload) {

    return encodeReleasingPayload(allocator, streamId, false, payload);
  }

  public static ByteBuf encodeNextCompleteReleasingPayload(
      ByteBufAllocator allocator, int streamId, Payload payload) {

    return encodeReleasingPayload(allocator, streamId, true, payload);
  }

  static ByteBuf encodeReleasingPayload(
      ByteBufAllocator allocator, int streamId, boolean complete, Payload payload) {

    return GenericFrameCodec.encodeReleasingPayload(
        allocator, FrameType.PAYLOAD, streamId, complete, true, payload);
  }

  public static ByteBuf encodeComplete(ByteBufAllocator allocator, int streamId) {
    return encode(allocator, streamId, false, true, false, null, null);
  }

  public static ByteBuf encode(
      ByteBufAllocator allocator,
      int streamId,
      boolean fragmentFollows,
      boolean complete,
      boolean next,
      @Nullable ByteBuf metadata,
      @Nullable ByteBuf data) {

    return GenericFrameCodec.encode(
        allocator, FrameType.PAYLOAD, streamId, fragmentFollows, complete, next, 0, metadata, data);
  }

  public static ByteBuf data(ByteBuf byteBuf) {
    return GenericFrameCodec.data(byteBuf);
  }

  @Nullable
  public static ByteBuf metadata(ByteBuf byteBuf) {
    return GenericFrameCodec.metadata(byteBuf);
  }
}
