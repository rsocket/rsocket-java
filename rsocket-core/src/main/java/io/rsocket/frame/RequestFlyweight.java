package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

class RequestFlyweight {
  FrameType frameType;

  RequestFlyweight(FrameType frameType) {
    this.frameType = frameType;
  }

  ByteBuf encode(
      final ByteBufAllocator allocator,
      final int streamId,
      boolean fragmentFollows,
      ByteBuf metadata,
      ByteBuf data) {
    return encode(allocator, streamId, fragmentFollows, false, false, 0, metadata, data);
  }

  ByteBuf encode(
      final ByteBufAllocator allocator,
      final int streamId,
      boolean fragmentFollows,
      boolean complete,
      boolean next,
      int requestN,
      ByteBuf metadata,
      ByteBuf data) {
    int flags = 0;

    if (metadata != null) {
      flags |= FrameHeaderFlyweight.FLAGS_M;
    }

    if (fragmentFollows) {
      flags |= FrameHeaderFlyweight.FLAGS_F;
    }

    if (complete) {
      flags |= FrameHeaderFlyweight.FLAGS_C;
    }

    if (next) {
      flags |= FrameHeaderFlyweight.FLAGS_N;
    }

    ByteBuf header = FrameHeaderFlyweight.encode(allocator, streamId, frameType, flags);

    if (requestN > 0) {
      header.writeInt(requestN);
    }

    if (metadata != null & data != null) {
      return DataAndMetadataFlyweight.encode(allocator, header, metadata, data);
    } else if (data != null) {
      return DataAndMetadataFlyweight.encodeOnlyData(allocator, header, data);
    } else {
      return header;
    }
  }

  ByteBuf data(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(FrameHeaderFlyweight.size());
    ByteBuf data = DataAndMetadataFlyweight.dataWithoutMarking(byteBuf);
    byteBuf.resetReaderIndex();
    return data;
  }

  ByteBuf metadata(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(FrameHeaderFlyweight.size());
    ByteBuf metadata = DataAndMetadataFlyweight.metadataWithoutMarking(byteBuf);
    byteBuf.resetReaderIndex();
    return metadata;
  }

  ByteBuf dataWithRequestN(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(FrameHeaderFlyweight.size() + Long.BYTES);
    ByteBuf data = DataAndMetadataFlyweight.dataWithoutMarking(byteBuf);
    byteBuf.resetReaderIndex();
    return data;
  }

  ByteBuf metadataWithRequestN(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(FrameHeaderFlyweight.size() + Long.BYTES);
    ByteBuf metadata = DataAndMetadataFlyweight.metadataWithoutMarking(byteBuf);
    byteBuf.resetReaderIndex();
    return metadata;
  }

  int initialRequestN(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    int i = byteBuf.skipBytes(FrameHeaderFlyweight.size()).readInt();
    byteBuf.resetReaderIndex();
    return i;
  }
}
