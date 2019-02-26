package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.reactivestreams.Subscriber;

/**
 * Per connection frame flyweight.
 *
 * <p>Not the latest frame layout, but close. Does not include - fragmentation / reassembly - encode
 * should remove Type param and have it as part of method name (1 encode per type?)
 *
 * <p>Not thread-safe. Assumed to be used single-threaded
 */
public final class FrameHeaderFlyweight {
  /** (I)gnore flag: a value of 0 indicates the protocol can't ignore this frame */
  public static final int FLAGS_I = 0b10_0000_0000;
  /** (M)etadata flag: a value of 1 indicates the frame contains metadata */
  public static final int FLAGS_M = 0b01_0000_0000;
  /**
   * (F)ollows: More fragments follow this fragment (in case of fragmented REQUEST_x or PAYLOAD
   * frames)
   */
  public static final int FLAGS_F = 0b00_1000_0000;
  /** (C)omplete: bit to indicate stream completion ({@link Subscriber#onComplete()}) */
  public static final int FLAGS_C = 0b00_0100_0000;
  /** (N)ext: bit to indicate payload or metadata present ({@link Subscriber#onNext(Object)}) */
  public static final int FLAGS_N = 0b00_0010_0000;

  public static final String DISABLE_FRAME_TYPE_CHECK = "io.rsocket.frames.disableFrameTypeCheck";
  private static final int FRAME_FLAGS_MASK = 0b0000_0011_1111_1111;
  private static final int FRAME_TYPE_BITS = 6;
  private static final int FRAME_TYPE_SHIFT = 16 - FRAME_TYPE_BITS;
  private static final int HEADER_SIZE = Integer.BYTES + Short.BYTES;
  private static boolean disableFrameTypeCheck;

  static {
    disableFrameTypeCheck = Boolean.getBoolean(DISABLE_FRAME_TYPE_CHECK);
  }

  private FrameHeaderFlyweight() {}

  static ByteBuf encodeStreamZero(
      final ByteBufAllocator allocator, final FrameType frameType, int flags) {
    return encode(allocator, 0, frameType, flags);
  }

  public static ByteBuf encode(
      final ByteBufAllocator allocator, final int streamId, final FrameType frameType, int flags) {
    if (!frameType.canHaveMetadata() && ((flags & FLAGS_M) == FLAGS_M)) {
      throw new IllegalStateException("bad value for metadata flag");
    }

    short typeAndFlags = (short) (frameType.getEncodedType() << FRAME_TYPE_SHIFT | (short) flags);

    return allocator.buffer().writeInt(streamId).writeShort(typeAndFlags);
  }

  public static boolean hasFollows(ByteBuf byteBuf) {
    return (flags(byteBuf) & FLAGS_F) == FLAGS_F;
  }

  public static int streamId(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    int streamId = byteBuf.readInt();
    byteBuf.resetReaderIndex();
    return streamId;
  }

  public static int flags(final ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(Integer.BYTES);
    short typeAndFlags = byteBuf.readShort();
    byteBuf.resetReaderIndex();
    return typeAndFlags & FRAME_FLAGS_MASK;
  }

  public static boolean hasMetadata(ByteBuf byteBuf) {
    return (flags(byteBuf) & FLAGS_M) == FLAGS_M;
  }

  public static FrameType frameType(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(Integer.BYTES);
    int typeAndFlags = byteBuf.readShort() & 0xFFFF;

    FrameType result = FrameType.fromEncodedType(typeAndFlags >> FRAME_TYPE_SHIFT);

    if (FrameType.PAYLOAD == result) {
      final int flags = typeAndFlags & FRAME_FLAGS_MASK;

      boolean complete = FLAGS_C == (flags & FLAGS_C);
      boolean next = FLAGS_N == (flags & FLAGS_N);
      if (next && complete) {
        result = FrameType.NEXT_COMPLETE;
      } else if (complete) {
        result = FrameType.COMPLETE;
      } else if (next) {
        result = FrameType.NEXT;
      } else {
        throw new IllegalArgumentException("Payload must set either or both of NEXT and COMPLETE.");
      }
    }

    byteBuf.resetReaderIndex();

    return result;
  }

  public static void ensureFrameType(final FrameType frameType, ByteBuf byteBuf) {
    if (!disableFrameTypeCheck) {
      final FrameType typeInFrame = frameType(byteBuf);

      if (typeInFrame != frameType) {
        throw new AssertionError("expected " + frameType + ", but saw " + typeInFrame);
      }
    }
  }

  public static int size() {
    return HEADER_SIZE;
  }
}
