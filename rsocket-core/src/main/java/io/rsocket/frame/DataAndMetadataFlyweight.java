package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

class DataAndMetadataFlyweight {
  public static final int FRAME_LENGTH_MASK = 0xFFFFFF;

  private DataAndMetadataFlyweight() {}

  private static void encodeLength(final ByteBuf byteBuf, final int length) {
    if ((length & ~FRAME_LENGTH_MASK) != 0) {
      throw new IllegalArgumentException("Length is larger than 24 bits");
    }
    // Write each byte separately in reverse order, this mean we can write 1 << 23 without
    // overflowing.
    byteBuf.writeByte(length >> 16);
    byteBuf.writeByte(length >> 8);
    byteBuf.writeByte(length);
  }

  private static int decodeLength(final ByteBuf byteBuf) {
    byte b = byteBuf.readByte();
    int length = (b & 0xFF) << 16;
    byte b1 = byteBuf.readByte();
    length |= (b1 & 0xFF) << 8;
    byte b2 = byteBuf.readByte();
    length |= b2 & 0xFF;
    return length;
  }

  static ByteBuf encodeOnlyMetadata(
      ByteBufAllocator allocator, final ByteBuf header, ByteBuf metadata) {
    return allocator.compositeBuffer(2).addComponents(true, header, metadata);
  }

  static ByteBuf encodeOnlyData(ByteBufAllocator allocator, final ByteBuf header, ByteBuf data) {
    return allocator.compositeBuffer(2).addComponents(true, header, data);
  }

  static ByteBuf encode(
      ByteBufAllocator allocator, final ByteBuf header, ByteBuf metadata, ByteBuf data) {

    int length = metadata.readableBytes();
    encodeLength(header, length);
    return allocator.compositeBuffer(3).addComponents(true, header, metadata, data);
  }

  static ByteBuf encode(ByteBufAllocator allocator, final ByteBuf header, ByteBuf metadata) {

    int length = metadata.readableBytes();
    encodeLength(header, length);
    return allocator.compositeBuffer(2).addComponents(true, header, metadata);
  }

  static ByteBuf metadataWithoutMarking(ByteBuf byteBuf, boolean hasMetadata) {
    if (hasMetadata) {
      int length = decodeLength(byteBuf);
      return byteBuf.readSlice(length);
    } else {
      return Unpooled.EMPTY_BUFFER;
    }
  }

  static ByteBuf metadata(ByteBuf byteBuf, boolean hasMetadata) {
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(6);
    ByteBuf metadata = metadataWithoutMarking(byteBuf, hasMetadata);
    byteBuf.resetReaderIndex();
    return metadata;
  }

  static ByteBuf dataWithoutMarking(ByteBuf byteBuf, boolean hasMetadata) {
    if (hasMetadata) {
      /*moves reader index*/
      int length = decodeLength(byteBuf);
      byteBuf.skipBytes(length);
    }
    if (byteBuf.readableBytes() > 0) {
      return byteBuf.readSlice(byteBuf.readableBytes());
    } else {
      return Unpooled.EMPTY_BUFFER;
    }
  }

  static ByteBuf data(ByteBuf byteBuf, boolean hasMetadata) {
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(6);
    ByteBuf data = dataWithoutMarking(byteBuf, hasMetadata);
    byteBuf.resetReaderIndex();
    return data;
  }
}
