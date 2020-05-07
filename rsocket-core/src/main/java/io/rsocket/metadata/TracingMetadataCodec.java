package io.rsocket.metadata;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class TracingMetadataCodec {

  static final int FLAG_EXTENDED_TRACE_ID_SIZE = 0b0000_1000;
  static final int FLAG_INCLUDE_PARENT_ID = 0b0000_0100;
  static final int FLAG_NOT_SAMPLED = 0b0001_0000;
  static final int FLAG_SAMPLED = 0b0010_0000;
  static final int FLAG_DEBUG = 0b0100_0000;
  static final int FLAG_IDS_SET = 0b1000_0000;

  public static ByteBuf encodeEmpty(ByteBufAllocator allocator, Flags flag) {

    return encode(allocator, true, 0, 0, false, 0, 0, false, flag);
  }

  public static ByteBuf encode128(
      ByteBufAllocator allocator,
      long traceIdHigh,
      long traceId,
      long spanId,
      long parentId,
      Flags flag) {

    return encode(allocator, false, traceIdHigh, traceId, true, spanId, parentId, true, flag);
  }

  public static ByteBuf encode128(
      ByteBufAllocator allocator, long traceIdHigh, long traceId, long spanId, Flags flag) {

    return encode(allocator, false, traceIdHigh, traceId, true, spanId, 0, false, flag);
  }

  public static ByteBuf encode64(
      ByteBufAllocator allocator, long traceId, long spanId, long parentId, Flags flag) {

    return encode(allocator, false, 0, traceId, false, spanId, parentId, true, flag);
  }

  public static ByteBuf encode64(
      ByteBufAllocator allocator, long traceId, long spanId, Flags flag) {
    return encode(allocator, false, 0, traceId, false, spanId, 0, false, flag);
  }

  static ByteBuf encode(
      ByteBufAllocator allocator,
      boolean isEmpty,
      long traceIdHigh,
      long traceId,
      boolean extendedTraceId,
      long spanId,
      long parentId,
      boolean includesParent,
      Flags flag) {
    int size =
        1
            + (isEmpty
                ? 0
                : (Long.BYTES
                    + Long.BYTES
                    + (extendedTraceId ? Long.BYTES : 0)
                    + (includesParent ? Long.BYTES : 0)));
    final ByteBuf buffer = allocator.buffer(size);

    int byteFlags = 0;
    switch (flag) {
      case NOT_SAMPLE:
        byteFlags |= FLAG_NOT_SAMPLED;
        break;
      case SAMPLE:
        byteFlags |= FLAG_SAMPLED;
        break;
      case DEBUG:
        byteFlags |= FLAG_DEBUG;
        break;
    }

    if (isEmpty) {
      return buffer.writeByte(byteFlags);
    }

    byteFlags |= FLAG_IDS_SET;

    if (extendedTraceId) {
      byteFlags |= FLAG_EXTENDED_TRACE_ID_SIZE;
    }

    if (includesParent) {
      byteFlags |= FLAG_INCLUDE_PARENT_ID;
    }

    buffer.writeByte(byteFlags);

    if (extendedTraceId) {
      buffer.writeLong(traceIdHigh);
    }

    buffer.writeLong(traceId).writeLong(spanId);

    if (includesParent) {
      buffer.writeLong(parentId);
    }

    return buffer;
  }

  public static TracingMetadata decode(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    try {
      byte flags = byteBuf.readByte();
      boolean isNotSampled = (flags & FLAG_NOT_SAMPLED) == FLAG_NOT_SAMPLED;
      boolean isSampled = (flags & FLAG_SAMPLED) == FLAG_SAMPLED;
      boolean isDebug = (flags & FLAG_DEBUG) == FLAG_DEBUG;
      boolean isIDSet = (flags & FLAG_IDS_SET) == FLAG_IDS_SET;

      if (!isIDSet) {
        return new TracingMetadata(0, 0, 0, false, 0, true, isNotSampled, isSampled, isDebug);
      }

      boolean extendedTraceId =
          (flags & FLAG_EXTENDED_TRACE_ID_SIZE) == FLAG_EXTENDED_TRACE_ID_SIZE;

      long traceIdHigh;
      if (extendedTraceId) {
        traceIdHigh = byteBuf.readLong();
      } else {
        traceIdHigh = 0;
      }

      long traceId = byteBuf.readLong();
      long spanId = byteBuf.readLong();

      boolean includesParent = (flags & FLAG_INCLUDE_PARENT_ID) == FLAG_INCLUDE_PARENT_ID;

      long parentId;
      if (includesParent) {
        parentId = byteBuf.readLong();
      } else {
        parentId = 0;
      }

      return new TracingMetadata(
          traceIdHigh,
          traceId,
          spanId,
          includesParent,
          parentId,
          false,
          isNotSampled,
          isSampled,
          isDebug);
    } finally {
      byteBuf.resetReaderIndex();
    }
  }

  public enum Flags {
    UNDECIDED,
    NOT_SAMPLE,
    SAMPLE,
    DEBUG
  }
}
