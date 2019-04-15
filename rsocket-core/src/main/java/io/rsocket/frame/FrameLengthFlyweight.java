package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.buffer.TupleByteBuffs;

/**
 * Some transports like TCP aren't framed, and require a length. This is used by DuplexConnections
 * for transports that need to send length
 */
public class FrameLengthFlyweight {
  public static final int FRAME_LENGTH_MASK = 0xFFFFFF;
  public static final int FRAME_LENGTH_SIZE = 3;

  private FrameLengthFlyweight() {}

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

  public static ByteBuf encode(ByteBufAllocator allocator, int length, ByteBuf frame) {
    ByteBuf buffer = allocator.buffer();
    encodeLength(buffer, length);
    return TupleByteBuffs.of(allocator, buffer, frame);
  }

  public static int length(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    int length = decodeLength(byteBuf);
    byteBuf.resetReaderIndex();
    return length;
  }

  public static ByteBuf frame(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(3);
    ByteBuf slice = byteBuf.slice();
    byteBuf.resetReaderIndex();
    return slice;
  }
}
