package io.rsocket.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.charset.Charset;

class Tuple2ByteBuf extends AbstractTupleByteBuf {

  private static final long ONE_MASK = 0x100000000L;
  private static final long TWO_MASK = 0x200000000L;
  private static final long MASK = 0x700000000L;

  private final ByteBuf one;
  private final ByteBuf two;
  private final int oneReadIndex;
  private final int twoReadIndex;
  private final int oneReadableBytes;
  private final int twoReadableBytes;
  private final int twoRelativeIndex;

  private boolean freed;

  Tuple2ByteBuf(ByteBufAllocator allocator, ByteBuf one, ByteBuf two) {
    super(allocator, one.readableBytes() + two.readableBytes());

    this.one = one;
    this.two = two;

    this.oneReadIndex = one.readerIndex();
    this.twoReadIndex = two.readerIndex();

    this.oneReadableBytes = one.readableBytes();
    this.twoReadableBytes = two.readableBytes();

    this.twoRelativeIndex = oneReadableBytes;

    this.freed = false;
  }

  long calculateRelativeIndex(int index) {
    checkIndex(index, 0);

    long relativeIndex;
    long mask;
    if (index >= twoRelativeIndex) {
      relativeIndex = twoReadIndex + (index - oneReadableBytes);
      mask = TWO_MASK;
    } else {
      relativeIndex = oneReadIndex + index;
      mask = ONE_MASK;
    }

    return relativeIndex | mask;
  }

  ByteBuf getPart(int index) {
    long ri = calculateRelativeIndex(index);
    switch ((int) ((ri & MASK) >>> 32L)) {
      case 0x1:
        return one;
      case 0x2:
        return two;
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  public boolean isDirect() {
    return one.isDirect() && two.isDirect();
  }

  @Override
  public int nioBufferCount() {
    return one.nioBufferCount() + two.nioBufferCount();
  }

  @Override
  public ByteBuffer nioBuffer() {
    ByteBuffer[] oneBuffers = one.nioBuffers();
    ByteBuffer[] twoBuffers = two.nioBuffers();

    ByteBuffer merged =
        BufferUtil.allocateDirectAligned(capacity, DEFAULT_DIRECT_MEMORY_CACHE_ALIGNMENT)
            .order(order());

    for (ByteBuffer b : oneBuffers) {
      merged.put(b);
    }

    for (ByteBuffer b : twoBuffers) {
      merged.put(b);
    }

    merged.flip();
    return merged;
  }

  @Override
  public ByteBuffer[] _nioBuffers(int index, int length) {
    long ri = calculateRelativeIndex(index);
    index = (int) (ri & Integer.MAX_VALUE);
    switch ((int) ((ri & MASK) >>> 32L)) {
      case 0x1:
        ByteBuffer[] oneBuffer;
        ByteBuffer[] twoBuffer;
        int l = Math.min(oneReadableBytes - index, length);
        oneBuffer = one.nioBuffers(index, l);
        length -= l;
        if (length != 0) {
          twoBuffer = two.nioBuffers(twoReadIndex, length);
          ByteBuffer[] results = new ByteBuffer[oneBuffer.length + twoBuffer.length];
          System.arraycopy(oneBuffer, 0, results, 0, oneBuffer.length);
          System.arraycopy(twoBuffer, 0, results, oneBuffer.length, twoBuffer.length);
          return results;
        } else {
          return oneBuffer;
        }
      case 0x2:
        return two.nioBuffers(index, length);
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
    long ri = calculateRelativeIndex(index);
    index = (int) (ri & Integer.MAX_VALUE);
    switch ((int) ((ri & MASK) >>> 32L)) {
      case 0x1:
        {
          int l = Math.min(oneReadableBytes - index, length);
          one.getBytes(index, dst, dstIndex, l);
          length -= l;
          dstIndex += l;

          if (length != 0) {
            two.getBytes(twoReadIndex, dst, dstIndex, length);
          }

          break;
        }
      case 0x2:
        {
          two.getBytes(index, dst, dstIndex, length);
          break;
        }
      default:
        throw new IllegalStateException();
    }

    return this;
  }

  @Override
  public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
    ByteBuf dstBuf = Unpooled.wrappedBuffer(dst);
    int min = Math.min(dst.length, capacity);
    return getBytes(0, dstBuf, index, min);
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuffer dst) {
    ByteBuf dstBuf = Unpooled.wrappedBuffer(dst);
    int min = Math.min(dst.limit(), capacity);
    return getBytes(0, dstBuf, index, min);
  }

  @Override
  public ByteBuf getBytes(int index, final OutputStream out, int length) throws IOException {
    checkIndex(index, length);
    long ri = calculateRelativeIndex(index);
    index = (int) (ri & Integer.MAX_VALUE);
    switch ((int) ((ri & MASK) >>> 32L)) {
      case 0x1:
        {
          int l = Math.min(oneReadableBytes - index, length);
          one.getBytes(index, out, l);
          length -= l;
          if (length != 0) {
            two.getBytes(twoReadIndex, out, length);
          }
          break;
        }
      case 0x2:
        {
          two.getBytes(index, out, length);
          break;
        }
      default:
        throw new IllegalStateException();
    }

    return this;
  }

  @Override
  public int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
    checkIndex(index, length);
    int read = 0;
    long ri = calculateRelativeIndex(index);
    index = (int) (ri & Integer.MAX_VALUE);
    switch ((int) ((ri & MASK) >>> 32L)) {
      case 0x1:
        {
          int l = Math.min(oneReadableBytes - index, length);
          read += one.getBytes(index, out, l);
          length -= l;
          if (length != 0) {
            read += two.getBytes(twoReadIndex, out, length);
          }
          break;
        }
      case 0x2:
        {
          read += two.getBytes(index, out, length);
          break;
        }
      default:
        throw new IllegalStateException();
    }

    return read;
  }

  @Override
  public int getBytes(int index, FileChannel out, long position, int length) throws IOException {
    checkIndex(index, length);
    int read = 0;
    long ri = calculateRelativeIndex(index);
    index = (int) (ri & Integer.MAX_VALUE);
    switch ((int) ((ri & MASK) >>> 32L)) {
      case 0x1:
        {
          int l = Math.min(oneReadableBytes - index, length);
          read += one.getBytes(index, out, position, l);
          length -= l;
          position += l;
          if (length != 0) {
            read += two.getBytes(twoReadIndex, out, position, length);
          }
          break;
        }
      case 0x2:
        {
          read += two.getBytes(index, out, position, length);
          break;
        }
      default:
        throw new IllegalStateException();
    }

    return read;
  }

  @Override
  public ByteBuf copy(int index, int length) {
    checkIndex(index, length);

    ByteBuf buffer = allocator.buffer(length);

    if (index == 0 && length == capacity) {
      buffer.writeBytes(one, oneReadIndex, oneReadableBytes);
      buffer.writeBytes(two, twoReadIndex, twoReadableBytes);

      return buffer;
    }

    long ri = calculateRelativeIndex(index);
    index = (int) (ri & Integer.MAX_VALUE);

    switch ((int) ((ri & MASK) >>> 32L)) {
      case 0x1:
        {
          int l = Math.min(oneReadableBytes - index, length);
          buffer.writeBytes(one, index, l);

          length -= l;

          if (length != 0) {
            buffer.writeBytes(two, twoReadIndex, length);
          }

          return buffer;
        }
      case 0x2:
        {
          return buffer.writeBytes(two, index, length);
        }
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  public ByteBuf slice(final int readIndex, int length) {
    checkIndex(readIndex, length);

    if (readIndex == 0 && length == capacity) {
      return new Tuple2ByteBuf(
          allocator,
          one.slice(oneReadIndex, oneReadableBytes),
          two.slice(twoReadIndex, twoReadableBytes));
    }

    long ri = calculateRelativeIndex(readIndex);
    int index = (int) (ri & Integer.MAX_VALUE);

    switch ((int) ((ri & MASK) >>> 32L)) {
      case 0x1:
        {
          ByteBuf oneSlice;
          ByteBuf twoSlice;

          int l = Math.min(oneReadableBytes - index, length);
          oneSlice = one.slice(index, l);
          length -= l;
          if (length != 0) {
            twoSlice = two.slice(twoReadIndex, length);
            return new Tuple2ByteBuf(allocator, oneSlice, twoSlice);
          } else {
            return oneSlice;
          }
        }
      case 0x2:
        {
          return two.slice(index, length);
        }
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  protected void deallocate() {
    if (freed) {
      return;
    }

    freed = true;
    ReferenceCountUtil.safeRelease(one);
    ReferenceCountUtil.safeRelease(two);
  }

  @Override
  public String toString(Charset charset) {
    StringBuilder builder = new StringBuilder(3);
    builder.append(one.toString(charset));
    builder.append(two.toString(charset));
    return builder.toString();
  }

  @Override
  public String toString(int index, int length, Charset charset) {
    // TODO - make this smarter
    return toString(charset).substring(index, length);
  }

  @Override
  public String toString() {
    return "Tuple2ByteBuf{"
        + "capacity="
        + capacity
        + ", one="
        + one
        + ", two="
        + two
        + ", allocator="
        + allocator
        + ", oneReadIndex="
        + oneReadIndex
        + ", twoReadIndex="
        + twoReadIndex
        + ", oneReadableBytes="
        + oneReadableBytes
        + ", twoReadableBytes="
        + twoReadableBytes
        + ", twoRelativeIndex="
        + twoRelativeIndex
        + '}';
  }
}
