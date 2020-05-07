package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.rsocket.RSocketErrorException;
import java.nio.charset.StandardCharsets;

public class ErrorFrameCodec {

  // defined zero stream id error codes
  public static final int INVALID_SETUP = 0x00000001;
  public static final int UNSUPPORTED_SETUP = 0x00000002;
  public static final int REJECTED_SETUP = 0x00000003;
  public static final int REJECTED_RESUME = 0x00000004;
  public static final int CONNECTION_ERROR = 0x00000101;
  public static final int CONNECTION_CLOSE = 0x00000102;
  // defined non-zero stream id error codes
  public static final int APPLICATION_ERROR = 0x00000201;
  public static final int REJECTED = 0x00000202;
  public static final int CANCELED = 0x00000203;
  public static final int INVALID = 0x00000204;
  // defined user-allowed error codes range
  public static final int MIN_USER_ALLOWED_ERROR_CODE = 0x00000301;
  public static final int MAX_USER_ALLOWED_ERROR_CODE = 0xFFFFFFFE;

  public static ByteBuf encode(
      ByteBufAllocator allocator, int streamId, Throwable t, ByteBuf data) {
    ByteBuf header = FrameHeaderCodec.encode(allocator, streamId, FrameType.ERROR, 0);

    int errorCode =
        t instanceof RSocketErrorException
            ? ((RSocketErrorException) t).errorCode()
            : APPLICATION_ERROR;

    header.writeInt(errorCode);

    return allocator.compositeBuffer(2).addComponents(true, header, data);
  }

  public static ByteBuf encode(ByteBufAllocator allocator, int streamId, Throwable t) {
    String message = t.getMessage() == null ? "" : t.getMessage();
    ByteBuf data = ByteBufUtil.writeUtf8(allocator, message);
    return encode(allocator, streamId, t, data);
  }

  public static int errorCode(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(FrameHeaderCodec.size());
    int i = byteBuf.readInt();
    byteBuf.resetReaderIndex();
    return i;
  }

  public static ByteBuf data(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(FrameHeaderCodec.size() + Integer.BYTES);
    ByteBuf slice = byteBuf.slice();
    byteBuf.resetReaderIndex();
    return slice;
  }

  public static String dataUtf8(ByteBuf byteBuf) {
    return data(byteBuf).toString(StandardCharsets.UTF_8);
  }
}
