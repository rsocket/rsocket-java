package io.rsocket.transport.aeron;

import io.netty.buffer.ByteBufUtil;
import io.rsocket.util.NumberUtils;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

final class SetupCodec {
  private SetupCodec() {}

  public static void encode(
      MutableDirectBuffer mutableDirectBuffer,
      int offset,
      long connectionId,
      int streamId,
      String channel,
      FrameType frameType) {

    PayloadCodec.encode(mutableDirectBuffer, offset, frameType);

    mutableDirectBuffer.putLong(offset + PayloadCodec.length(), connectionId);
    mutableDirectBuffer.putInt(offset + PayloadCodec.length() + Long.BYTES, streamId);

    int channelUtf8Length = NumberUtils.requireUnsignedShort(ByteBufUtil.utf8Bytes(channel));
    mutableDirectBuffer.putStringUtf8(
        offset + PayloadCodec.length() + Long.BYTES + Integer.BYTES, channel, channelUtf8Length);
  }

  public static long connectionId(DirectBuffer directBuffer, int bufferOffset) {
    int offset = bufferOffset + PayloadCodec.length();
    return directBuffer.getLong(offset);
  }

  public static int streamId(DirectBuffer directBuffer, int bufferOffset) {
    int offset = bufferOffset + PayloadCodec.length() + Long.BYTES;
    return directBuffer.getInt(offset);
  }

  public static String channel(DirectBuffer directBuffer, int bufferOffset) {
    int offset = bufferOffset + PayloadCodec.length() + Long.BYTES + Integer.BYTES;

    return directBuffer.getStringUtf8(offset);
  }

  public static int constantLength() {
    return PayloadCodec.length() + Long.BYTES + Integer.BYTES + Integer.BYTES;
  }
}
