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

  static ByteBuf encode(
      ByteBufAllocator allocator,
      final ByteBuf header,
      ByteBuf metadata,
      boolean hasMetadata,
      ByteBuf data) {

    final boolean addData = data != null && data.isReadable();
    final boolean addMetadata = hasMetadata && metadata.isReadable();

    if (hasMetadata) {
      int length = metadata.readableBytes();
      encodeLength(header, length);
    }

    if (addMetadata && addData) {
      return allocator.compositeBuffer(3).addComponents(true, header, metadata, data);
    } else if (addMetadata) {
      return allocator.compositeBuffer(2).addComponents(true, header, metadata);
    } else if (addData) {
      return allocator.compositeBuffer(2).addComponents(true, header, data);
    } else {
      return header;
    }
  }

  static ByteBuf metadataWithoutMarking(ByteBuf byteBuf) {
    int length = decodeLength(byteBuf);
    return byteBuf.readSlice(length);
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
}
