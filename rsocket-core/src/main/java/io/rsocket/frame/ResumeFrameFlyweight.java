package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class ResumeFrameFlyweight {
  static final int CURRENT_VERSION = SetupFrameFlyweight.CURRENT_VERSION;

  public static ByteBuf encode(
      final ByteBufAllocator allocator,
      byte[] token,
      long lastReceivedServerPos,
      long firstAvailableClientPos) {

    ByteBuf byteBuf = FrameHeaderFlyweight.encodeStreamZero(allocator, FrameType.RESUME, 0);
    byteBuf.writeInt(CURRENT_VERSION);
    byteBuf.writeShort(token.length);
    byteBuf.writeBytes(token);
    byteBuf.writeLong(lastReceivedServerPos);
    byteBuf.writeLong(firstAvailableClientPos);

    return byteBuf;
  }

  public static int version(ByteBuf byteBuf) {
    FrameHeaderFlyweight.ensureFrameType(FrameType.RESUME, byteBuf);

    byteBuf.markReaderIndex();
    byteBuf.skipBytes(FrameHeaderFlyweight.size());
    int version = byteBuf.readInt();
    byteBuf.resetReaderIndex();

    return version;
  }

  public static byte[] token(ByteBuf byteBuf) {
    FrameHeaderFlyweight.ensureFrameType(FrameType.RESUME, byteBuf);

    byteBuf.markReaderIndex();
    // header + version
    int tokenPos = FrameHeaderFlyweight.size() + Integer.BYTES;
    byteBuf.skipBytes(tokenPos);
    //token
    int tokenLength = byteBuf.readShort() & 0xFFFF;
    byte[] token = new byte[tokenLength];
    byteBuf.readBytes(token);
    byteBuf.resetReaderIndex();

    return token;
  }

  public static long lastReceivedServerPos(ByteBuf byteBuf) {
    FrameHeaderFlyweight.ensureFrameType(FrameType.RESUME, byteBuf);

    byteBuf.markReaderIndex();
    // header + version
    int tokenPos = FrameHeaderFlyweight.size() + Integer.BYTES;
    byteBuf.skipBytes(tokenPos);
    //token
    int tokenLength = byteBuf.readShort() & 0xFFFF;
    byteBuf.skipBytes(tokenLength);
    long lastReceivedServerPos = byteBuf.readLong();
    byteBuf.resetReaderIndex();

    return lastReceivedServerPos;
  }

  public static long firstAvailableClientPos(ByteBuf byteBuf) {
    FrameHeaderFlyweight.ensureFrameType(FrameType.RESUME, byteBuf);

    byteBuf.markReaderIndex();
    // header + version
    int tokenPos = FrameHeaderFlyweight.size() + Integer.BYTES;
    byteBuf.skipBytes(tokenPos);
    //token
    int tokenLength = byteBuf.readShort() & 0xFFFF;
    byteBuf.skipBytes(tokenLength);
    // last received server position
    byteBuf.skipBytes(Long.BYTES);
    long firstAvailableClientPos = byteBuf.readLong();
    byteBuf.resetReaderIndex();

    return firstAvailableClientPos;
  }
}
