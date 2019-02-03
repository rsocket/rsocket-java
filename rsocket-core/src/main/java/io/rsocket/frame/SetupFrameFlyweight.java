package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;

public class SetupFrameFlyweight {
  /**
   * A flag used to indicate that the client requires connection resumption, if possible (the frame
   * contains a Resume Identification Token)
   */
  public static final int FLAGS_RESUME_ENABLE = 0b00_1000_0000;

  /** A flag used to indicate that the client will honor LEASE sent by the server */
  public static final int FLAGS_WILL_HONOR_LEASE = 0b00_0100_0000;

  public static final int CURRENT_VERSION = VersionFlyweight.encode(1, 0);

  public static ByteBuf encode(
      final ByteBufAllocator allocator,
      boolean lease,
      boolean resume,
      final int keepaliveInterval,
      final int maxLifetime,
      final String metadataMimeType,
      final String dataMimeType,
      final ByteBuf metadata,
      final ByteBuf data) {
    return encode(
        allocator,
        lease,
        resume,
        keepaliveInterval,
        maxLifetime,
        Unpooled.EMPTY_BUFFER,
        metadataMimeType,
        dataMimeType,
        metadata,
        data);
  }

  public static ByteBuf encode(
      final ByteBufAllocator allocator,
      boolean lease,
      boolean resume,
      final int keepaliveInterval,
      final int maxLifetime,
      final ByteBuf resumeToken,
      final String metadataMimeType,
      final String dataMimeType,
      final ByteBuf metadata,
      final ByteBuf data) {

    int flags = 0;
    if (resume) {
      throw new IllegalArgumentException("RESUME_ENABLE not supported");
    }

    /*
    if (resume) {
      flags |= FLAGS_RESUME_ENABLE;
    }*/

    if (lease) {
      flags |= FLAGS_WILL_HONOR_LEASE;
    }

    if (metadata != null) {
      flags |= FrameHeaderFlyweight.FLAGS_M;
    }

    ByteBuf header = FrameHeaderFlyweight.encodeStreamZero(allocator, FrameType.SETUP, flags);

    header.writeInt(CURRENT_VERSION).writeInt(keepaliveInterval).writeInt(maxLifetime);

    if ((flags & FLAGS_RESUME_ENABLE) != 0) {
      header.writeShort(resumeToken.readableBytes()).writeBytes(resumeToken);
    }

    // Write metadata mime-type
    int length = ByteBufUtil.utf8Bytes(metadataMimeType);
    header.writeByte(length);
    ByteBufUtil.writeUtf8(header, metadataMimeType);

    // Write data mime-type
    length = ByteBufUtil.utf8Bytes(dataMimeType);
    header.writeByte(length);
    ByteBufUtil.writeUtf8(header, dataMimeType);

    return DataAndMetadataFlyweight.encode(allocator, header, metadata, data);
  }

  public static int version(ByteBuf byteBuf) {
    FrameHeaderFlyweight.ensureFrameType(FrameType.SETUP, byteBuf);
    byteBuf.markReaderIndex();
    int i = byteBuf.skipBytes(FrameHeaderFlyweight.size()).readInt();
    byteBuf.resetReaderIndex();
    return i;
  }

  public static int resumeTokenLength(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    short i = byteBuf.skipBytes(FrameHeaderFlyweight.size()).readShort();
    byteBuf.resetReaderIndex();
    return i;
  }

  private static int bytesToSkipToMimeType(ByteBuf byteBuf) {
    int bytesToSkip = FrameHeaderFlyweight.size();
    if ((FLAGS_RESUME_ENABLE & FrameHeaderFlyweight.flags(byteBuf)) == FLAGS_RESUME_ENABLE) {
      bytesToSkip = resumeTokenLength(byteBuf) + Short.BYTES;
    }
    bytesToSkip +=
        // Skip Version
        Integer.BYTES
            // Skip keepaliveInterval
            + Integer.BYTES
            // Skip maxLifetime
            + Integer.BYTES;

    byteBuf.resetReaderIndex();
    return bytesToSkip;
  }

  public static boolean honorLease(ByteBuf byteBuf) {
    return (FLAGS_WILL_HONOR_LEASE & FrameHeaderFlyweight.flags(byteBuf)) == FLAGS_WILL_HONOR_LEASE;
  }

  public static boolean resumeEnabled(ByteBuf byteBuf) {
    return (FLAGS_RESUME_ENABLE & FrameHeaderFlyweight.flags(byteBuf)) == FLAGS_RESUME_ENABLE;
  }

  public static String metadataMimeType(ByteBuf byteBuf) {
    int skip = bytesToSkipToMimeType(byteBuf);
    byteBuf.markReaderIndex();
    int length = byteBuf.skipBytes(skip).readByte();
    String s = byteBuf.readSlice(length).toString(StandardCharsets.UTF_8);
    byteBuf.resetReaderIndex();
    return s;
  }

  public static String dataMimeType(ByteBuf byteBuf) {
    int skip = bytesToSkipToMimeType(byteBuf);
    byteBuf.markReaderIndex();
    int length = byteBuf.skipBytes(skip).readByte();
    length = byteBuf.skipBytes(length).readByte();
    String s = byteBuf.readSlice(length).toString(StandardCharsets.UTF_8);
    byteBuf.resetReaderIndex();
    return s;
  }

  public static ByteBuf metadata(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    skipToPayload(byteBuf);
    ByteBuf metadata = DataAndMetadataFlyweight.metadataWithoutMarking(byteBuf);
    byteBuf.resetReaderIndex();
    return metadata;
  }

  public static ByteBuf data(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    skipToPayload(byteBuf);
    ByteBuf data = DataAndMetadataFlyweight.dataWithoutMarking(byteBuf);
    byteBuf.resetReaderIndex();
    return data;
  }

  private static void skipToPayload(ByteBuf byteBuf) {
    int skip = bytesToSkipToMimeType(byteBuf);
    byte length = byteBuf.skipBytes(skip).readByte();
    length = byteBuf.skipBytes(length).readByte();
    byteBuf.skipBytes(length);
  }
}
