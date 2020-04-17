package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
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

  private static final int VERSION_FIELD_OFFSET = FrameHeaderFlyweight.size();
  private static final int KEEPALIVE_INTERVAL_FIELD_OFFSET = VERSION_FIELD_OFFSET + Integer.BYTES;
  private static final int KEEPALIVE_MAX_LIFETIME_FIELD_OFFSET =
      KEEPALIVE_INTERVAL_FIELD_OFFSET + Integer.BYTES;
  private static final int VARIABLE_DATA_OFFSET =
      KEEPALIVE_MAX_LIFETIME_FIELD_OFFSET + Integer.BYTES;

  public static ByteBuf encode(
      final ByteBufAllocator allocator,
      final boolean lease,
      final int keepaliveInterval,
      final int maxLifetime,
      final String metadataMimeType,
      final String dataMimeType,
      final Payload setupPayload) {
    return encode(
        allocator,
        lease,
        keepaliveInterval,
        maxLifetime,
        Unpooled.EMPTY_BUFFER,
        metadataMimeType,
        dataMimeType,
        setupPayload);
  }

  public static ByteBuf encode(
      final ByteBufAllocator allocator,
      final boolean lease,
      final int keepaliveInterval,
      final int maxLifetime,
      final ByteBuf resumeToken,
      final String metadataMimeType,
      final String dataMimeType,
      final Payload setupPayload) {

    final ByteBuf data = setupPayload.sliceData();
    final boolean hasData = data.isReadable();
    final boolean hasMetadata = setupPayload.hasMetadata();
    final ByteBuf metadata = hasMetadata ? setupPayload.sliceMetadata() : null;

    int flags = 0;

    if (resumeToken != null && resumeToken.readableBytes() > 0) {
      flags |= FLAGS_RESUME_ENABLE;
    }

    if (lease) {
      flags |= FLAGS_WILL_HONOR_LEASE;
    }

    if (hasMetadata) {
      flags |= FrameHeaderFlyweight.FLAGS_M;
    }

    final ByteBuf header = FrameHeaderFlyweight.encodeStreamZero(allocator, FrameType.SETUP, flags);

    header.writeInt(CURRENT_VERSION).writeInt(keepaliveInterval).writeInt(maxLifetime);

    if ((flags & FLAGS_RESUME_ENABLE) != 0) {
      resumeToken.markReaderIndex();
      header.writeShort(resumeToken.readableBytes()).writeBytes(resumeToken);
      resumeToken.resetReaderIndex();
    }

    // Write metadata mime-type
    int length = ByteBufUtil.utf8Bytes(metadataMimeType);
    header.writeByte(length);
    ByteBufUtil.writeUtf8(header, metadataMimeType);

    // Write data mime-type
    length = ByteBufUtil.utf8Bytes(dataMimeType);
    header.writeByte(length);
    ByteBufUtil.writeUtf8(header, dataMimeType);

    if (hasData && hasMetadata) {
      return DataAndMetadataFlyweight.encode(allocator, header, metadata, data);
    } else if (hasMetadata) {
      return DataAndMetadataFlyweight.encode(allocator, header, metadata);
    } else if (hasData) {
      return DataAndMetadataFlyweight.encodeOnlyData(allocator, header, data);
    } else {
      return header;
    }
  }

  public static int version(ByteBuf byteBuf) {
    FrameHeaderFlyweight.ensureFrameType(FrameType.SETUP, byteBuf);
    byteBuf.markReaderIndex();
    int version = byteBuf.skipBytes(VERSION_FIELD_OFFSET).readInt();
    byteBuf.resetReaderIndex();
    return version;
  }

  public static String humanReadableVersion(ByteBuf byteBuf) {
    int encodedVersion = version(byteBuf);
    return VersionFlyweight.major(encodedVersion) + "." + VersionFlyweight.minor(encodedVersion);
  }

  public static boolean isSupportedVersion(ByteBuf byteBuf) {
    return CURRENT_VERSION == version(byteBuf);
  }

  public static int resumeTokenLength(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    int tokenLength = byteBuf.skipBytes(VARIABLE_DATA_OFFSET).readShort() & 0xFFFF;
    byteBuf.resetReaderIndex();
    return tokenLength;
  }

  public static int keepAliveInterval(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    int keepAliveInterval = byteBuf.skipBytes(KEEPALIVE_INTERVAL_FIELD_OFFSET).readInt();
    byteBuf.resetReaderIndex();
    return keepAliveInterval;
  }

  public static int keepAliveMaxLifetime(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    int keepAliveMaxLifetime = byteBuf.skipBytes(KEEPALIVE_MAX_LIFETIME_FIELD_OFFSET).readInt();
    byteBuf.resetReaderIndex();
    return keepAliveMaxLifetime;
  }

  public static boolean honorLease(ByteBuf byteBuf) {
    return (FLAGS_WILL_HONOR_LEASE & FrameHeaderFlyweight.flags(byteBuf)) == FLAGS_WILL_HONOR_LEASE;
  }

  public static boolean resumeEnabled(ByteBuf byteBuf) {
    return (FLAGS_RESUME_ENABLE & FrameHeaderFlyweight.flags(byteBuf)) == FLAGS_RESUME_ENABLE;
  }

  public static ByteBuf resumeToken(ByteBuf byteBuf) {
    if (resumeEnabled(byteBuf)) {
      byteBuf.markReaderIndex();
      // header
      int resumePos =
          FrameHeaderFlyweight.size()
              +
              // version
              Integer.BYTES
              +
              // keep-alive interval
              Integer.BYTES
              +
              // keep-alive maxLifeTime
              Integer.BYTES;

      int tokenLength = byteBuf.skipBytes(resumePos).readShort() & 0xFFFF;
      ByteBuf resumeToken = byteBuf.readSlice(tokenLength);
      byteBuf.resetReaderIndex();
      return resumeToken;
    } else {
      return null;
    }
  }

  public static String metadataMimeType(ByteBuf byteBuf) {
    int skip = bytesToSkipToMimeType(byteBuf);
    byteBuf.markReaderIndex();
    int length = byteBuf.skipBytes(skip).readUnsignedByte();
    String mimeType = byteBuf.slice(byteBuf.readerIndex(), length).toString(StandardCharsets.UTF_8);
    byteBuf.resetReaderIndex();
    return mimeType;
  }

  public static String dataMimeType(ByteBuf byteBuf) {
    int skip = bytesToSkipToMimeType(byteBuf);
    byteBuf.markReaderIndex();
    int metadataLength = byteBuf.skipBytes(skip).readByte();
    int dataLength = byteBuf.skipBytes(metadataLength).readByte();
    String mimeType = byteBuf.readSlice(dataLength).toString(StandardCharsets.UTF_8);
    byteBuf.resetReaderIndex();
    return mimeType;
  }

  public static ByteBuf metadata(ByteBuf byteBuf) {
    boolean hasMetadata = FrameHeaderFlyweight.hasMetadata(byteBuf);
    byteBuf.markReaderIndex();
    skipToPayload(byteBuf);
    ByteBuf metadata = DataAndMetadataFlyweight.metadataWithoutMarking(byteBuf, hasMetadata);
    byteBuf.resetReaderIndex();
    return metadata;
  }

  public static ByteBuf data(ByteBuf byteBuf) {
    boolean hasMetadata = FrameHeaderFlyweight.hasMetadata(byteBuf);
    byteBuf.markReaderIndex();
    skipToPayload(byteBuf);
    ByteBuf data = DataAndMetadataFlyweight.dataWithoutMarking(byteBuf, hasMetadata);
    byteBuf.resetReaderIndex();
    return data;
  }

  private static int bytesToSkipToMimeType(ByteBuf byteBuf) {
    int bytesToSkip = VARIABLE_DATA_OFFSET;
    if ((FLAGS_RESUME_ENABLE & FrameHeaderFlyweight.flags(byteBuf)) == FLAGS_RESUME_ENABLE) {
      bytesToSkip += resumeTokenLength(byteBuf) + Short.BYTES;
    }
    return bytesToSkip;
  }

  private static void skipToPayload(ByteBuf byteBuf) {
    int skip = bytesToSkipToMimeType(byteBuf);
    byte length = byteBuf.skipBytes(skip).readByte();
    length = byteBuf.skipBytes(length).readByte();
    byteBuf.skipBytes(length);
  }
}
