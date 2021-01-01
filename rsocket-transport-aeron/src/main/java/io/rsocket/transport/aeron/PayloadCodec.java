package io.rsocket.transport.aeron;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

final class PayloadCodec {
  private static final short VERSION = 0;

  private PayloadCodec() {}

  public static FrameType frameType(DirectBuffer byteBuf, int offset) {
    int intFrameType = byteBuf.getInt(offset + Short.BYTES);

    return FrameType.fromEncodedType(intFrameType);
  }

  public static short version(DirectBuffer directBuffer, int offset) {
    return directBuffer.getShort(offset);
  }

  public static void encode(MutableDirectBuffer directBuffer, int offset, FrameType type) {
    directBuffer.putShort(offset, VERSION);
    directBuffer.putInt(offset + Short.BYTES, type.getEncodedType());
  }

  public static int length() {
    return Short.BYTES + Integer.BYTES;
  }
}
