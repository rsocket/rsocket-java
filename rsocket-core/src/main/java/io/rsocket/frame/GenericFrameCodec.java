package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.IllegalReferenceCountException;
import io.rsocket.Payload;
import reactor.util.annotation.Nullable;

class GenericFrameCodec {

  static ByteBuf encodeReleasingPayload(
      final ByteBufAllocator allocator,
      final FrameType frameType,
      final int streamId,
      boolean complete,
      boolean next,
      final Payload payload) {
    return encodeReleasingPayload(allocator, frameType, streamId, complete, next, 0, payload);
  }

  static ByteBuf encodeReleasingPayload(
      final ByteBufAllocator allocator,
      final FrameType frameType,
      final int streamId,
      boolean complete,
      boolean next,
      int requestN,
      final Payload payload) {

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

    return encode(allocator, frameType, streamId, false, complete, next, requestN, metadata, data);
  }

  static ByteBuf encode(
      final ByteBufAllocator allocator,
      final FrameType frameType,
      final int streamId,
      boolean fragmentFollows,
      @Nullable ByteBuf metadata,
      ByteBuf data) {
    return encode(allocator, frameType, streamId, fragmentFollows, false, false, 0, metadata, data);
  }

  static ByteBuf encode(
      final ByteBufAllocator allocator,
      final FrameType frameType,
      final int streamId,
      boolean fragmentFollows,
      boolean complete,
      boolean next,
      int requestN,
      @Nullable ByteBuf metadata,
      @Nullable ByteBuf data) {

    final boolean hasMetadata = metadata != null;

    int flags = 0;

    if (hasMetadata) {
      flags |= FrameHeaderCodec.FLAGS_M;
    }

    if (fragmentFollows) {
      flags |= FrameHeaderCodec.FLAGS_F;
    }

    if (complete) {
      flags |= FrameHeaderCodec.FLAGS_C;
    }

    if (next) {
      flags |= FrameHeaderCodec.FLAGS_N;
    }

    final ByteBuf header = FrameHeaderCodec.encode(allocator, streamId, frameType, flags);

    if (requestN > 0) {
      header.writeInt(requestN);
    }

    return FrameBodyCodec.encode(allocator, header, metadata, hasMetadata, data);
  }

  static ByteBuf data(ByteBuf byteBuf) {
    boolean hasMetadata = FrameHeaderCodec.hasMetadata(byteBuf);
    int idx = byteBuf.readerIndex();
    byteBuf.skipBytes(FrameHeaderCodec.size());
    ByteBuf data = FrameBodyCodec.dataWithoutMarking(byteBuf, hasMetadata);
    byteBuf.readerIndex(idx);
    return data;
  }

  @Nullable
  static ByteBuf metadata(ByteBuf byteBuf) {
    boolean hasMetadata = FrameHeaderCodec.hasMetadata(byteBuf);
    if (!hasMetadata) {
      return null;
    }
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(FrameHeaderCodec.size());
    ByteBuf metadata = FrameBodyCodec.metadataWithoutMarking(byteBuf);
    byteBuf.resetReaderIndex();
    return metadata;
  }

  static ByteBuf dataWithRequestN(ByteBuf byteBuf) {
    boolean hasMetadata = FrameHeaderCodec.hasMetadata(byteBuf);
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(FrameHeaderCodec.size() + Integer.BYTES);
    ByteBuf data = FrameBodyCodec.dataWithoutMarking(byteBuf, hasMetadata);
    byteBuf.resetReaderIndex();
    return data;
  }

  @Nullable
  static ByteBuf metadataWithRequestN(ByteBuf byteBuf) {
    boolean hasMetadata = FrameHeaderCodec.hasMetadata(byteBuf);
    if (!hasMetadata) {
      return null;
    }
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(FrameHeaderCodec.size() + Integer.BYTES);
    ByteBuf metadata = FrameBodyCodec.metadataWithoutMarking(byteBuf);
    byteBuf.resetReaderIndex();
    return metadata;
  }

  static int initialRequestN(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    int i = byteBuf.skipBytes(FrameHeaderCodec.size()).readInt();
    byteBuf.resetReaderIndex();
    return i;
  }
}
