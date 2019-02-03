package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

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
    int length = (byteBuf.readByte() & 0xFF) << 16;
    length |= (byteBuf.readByte() & 0xFF) << 8;
    length |= byteBuf.readByte() & 0xFF;
    return length;
  }

  static ByteBuf encodeOnlyMetadata(
      ByteBufAllocator allocator, final ByteBuf header, ByteBuf metadata) {
    int length = metadata.readableBytes();
    encodeLength(header, length);

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

  static ByteBuf metadataWithoutMarking(ByteBuf byteBuf) {
    int length = decodeLength(byteBuf);
    ByteBuf metadata = byteBuf.readSlice(length);
    return metadata;
  }

  static ByteBuf metadata(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    int length = decodeLength(byteBuf);
    ByteBuf metadata = byteBuf.readSlice(length);
    byteBuf.resetReaderIndex();
    return metadata;
  }

  static ByteBuf dataWithoutMarking(ByteBuf byteBuf) {
    int length = decodeLength(byteBuf);
    byteBuf.skipBytes(length);
    return byteBuf.slice();
  }

  static ByteBuf data(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    ByteBuf data = dataWithoutMarking(byteBuf);
    byteBuf.resetReaderIndex();
    return data;
  }
}
