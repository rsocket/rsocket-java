package io.rsocket.buffer;

import io.netty.buffer.AbstractReferenceCountedByteBuf;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.internal.SystemPropertyUtil;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;

abstract class AbstractTupleByteBuf extends AbstractReferenceCountedByteBuf {
  static final int DEFAULT_DIRECT_MEMORY_CACHE_ALIGNMENT =
      SystemPropertyUtil.getInt("io.netty.allocator.directMemoryCacheAlignment", 0);
  static final ByteBuffer EMPTY_NIO_BUFFER = Unpooled.EMPTY_BUFFER.nioBuffer();

  final ByteBufAllocator allocator;
  final int capacity;

  AbstractTupleByteBuf(ByteBufAllocator allocator, int capacity) {
    super(Integer.MAX_VALUE);

    this.capacity = capacity;
    this.allocator = allocator;
    super.writerIndex(capacity);
  }

  abstract long calculateRelativeIndex(int index);

  abstract ByteBuf getPart(int index);

  @Override
  public ByteBuffer nioBuffer(int index, int length) {
    checkIndex(index, length);

    ByteBuffer[] buffers = nioBuffers(index, length);

    if (buffers.length == 1) {
      return buffers[0].duplicate();
    }

    ByteBuffer merged =
        BufferUtil.allocateDirectAligned(length, DEFAULT_DIRECT_MEMORY_CACHE_ALIGNMENT)
            .order(order());
    for (ByteBuffer buf : buffers) {
      merged.put(buf);
    }

    merged.flip();
    return merged;
  }

  @Override
  public ByteBuffer[] nioBuffers(int index, int length) {
    checkIndex(index, length);
    if (length == 0) {
      return new ByteBuffer[] {EMPTY_NIO_BUFFER};
    }
    return _nioBuffers(index, length);
  }

  protected abstract ByteBuffer[] _nioBuffers(int index, int length);

  @Override
  protected byte _getByte(final int index) {
    long ri = calculateRelativeIndex(index);
    ByteBuf byteBuf = getPart(index);

    int calculatedIndex = (int) (ri & Integer.MAX_VALUE);

    return byteBuf.getByte(calculatedIndex);
  }

  @Override
  protected short _getShort(final int index) {
    long ri = calculateRelativeIndex(index);
    ByteBuf byteBuf = getPart(index);

    final int calculatedIndex = (int) (ri & Integer.MAX_VALUE);

    if (calculatedIndex + Short.BYTES <= byteBuf.writerIndex()) {
      return byteBuf.getShort(calculatedIndex);
    } else if (order() == ByteOrder.BIG_ENDIAN) {
      return (short) ((_getByte(index) & 0xff) << 8 | _getByte(index + 1) & 0xff);
    } else {
      return (short) (_getByte(index) & 0xff | (_getByte(index + 1) & 0xff) << 8);
    }
  }

  @Override
  protected short _getShortLE(int index) {
    long ri = calculateRelativeIndex(index);
    ByteBuf byteBuf = getPart(index);

    final int calculatedIndex = (int) (ri & Integer.MAX_VALUE);

    if (calculatedIndex + Short.BYTES <= byteBuf.writerIndex()) {
      return byteBuf.getShortLE(calculatedIndex);
    } else if (order() == ByteOrder.BIG_ENDIAN) {
      return (short) (_getByte(index) & 0xff | (_getByte(index + 1) & 0xff) << 8);
    } else {
      return (short) ((_getByte(index) & 0xff) << 8 | _getByte(index + 1) & 0xff);
    }
  }

  @Override
  protected int _getUnsignedMedium(final int index) {
    long ri = calculateRelativeIndex(index);
    ByteBuf byteBuf = getPart(index);

    int calculatedIndex = (int) (ri & Integer.MAX_VALUE);

    if (calculatedIndex + 3 <= byteBuf.writerIndex()) {
      return byteBuf.getUnsignedMedium(calculatedIndex);
    } else if (order() == ByteOrder.BIG_ENDIAN) {
      return (_getShort(index) & 0xffff) << 8 | _getByte(index + 2) & 0xff;
    } else {
      return _getShort(index) & 0xFFFF | (_getByte(index + 2) & 0xFF) << 16;
    }
  }

  @Override
  protected int _getUnsignedMediumLE(int index) {
    long ri = calculateRelativeIndex(index);
    ByteBuf byteBuf = getPart(index);

    int calculatedIndex = (int) (ri & Integer.MAX_VALUE);

    if (calculatedIndex + 3 <= byteBuf.writerIndex()) {
      return byteBuf.getUnsignedMediumLE(calculatedIndex);
    } else if (order() == ByteOrder.BIG_ENDIAN) {
      return _getShortLE(index) & 0xffff | (_getByte(index + 2) & 0xff) << 16;
    } else {
      return (_getShortLE(index) & 0xffff) << 8 | _getByte(index + 2) & 0xff;
    }
  }

  @Override
  protected int _getInt(final int index) {
    long ri = calculateRelativeIndex(index);
    ByteBuf byteBuf = getPart(index);

    int calculatedIndex = (int) (ri & Integer.MAX_VALUE);

    if (calculatedIndex + Integer.BYTES <= byteBuf.writerIndex()) {
      return byteBuf.getInt(calculatedIndex);
    } else if (order() == ByteOrder.BIG_ENDIAN) {
      return (_getShort(index) & 0xffff) << 16 | _getShort(index + 2) & 0xffff;
    } else {
      return _getShort(index) & 0xFFFF | (_getShort(index + 2) & 0xFFFF) << 16;
    }
  }

  @Override
  protected int _getIntLE(final int index) {
    long ri = calculateRelativeIndex(index);
    ByteBuf byteBuf = getPart(index);

    int calculatedIndex = (int) (ri & Integer.MAX_VALUE);

    if (calculatedIndex + Integer.BYTES <= byteBuf.writerIndex()) {
      return byteBuf.getIntLE(calculatedIndex);
    } else if (order() == ByteOrder.BIG_ENDIAN) {
      return _getShortLE(index) & 0xffff | (_getShortLE(index + 2) & 0xffff) << 16;
    } else {
      return (_getShortLE(index) & 0xffff) << 16 | _getShortLE(index + 2) & 0xffff;
    }
  }

  @Override
  protected long _getLong(final int index) {
    long ri = calculateRelativeIndex(index);
    ByteBuf byteBuf = getPart(index);

    int calculatedIndex = (int) (ri & Integer.MAX_VALUE);

    if (calculatedIndex + Long.BYTES <= byteBuf.writerIndex()) {
      return byteBuf.getLong(calculatedIndex);
    } else if (order() == ByteOrder.BIG_ENDIAN) {
      return (_getInt(index) & 0xffffffffL) << 32 | _getInt(index + 4) & 0xffffffffL;
    } else {
      return _getInt(index) & 0xFFFFFFFFL | (_getInt(index + 4) & 0xFFFFFFFFL) << 32;
    }
  }

  @Override
  protected long _getLongLE(final int index) {
    long ri = calculateRelativeIndex(index);
    ByteBuf byteBuf = getPart(index);

    int calculatedIndex = (int) (ri & Integer.MAX_VALUE);

    if (calculatedIndex + Long.BYTES <= byteBuf.writerIndex()) {
      return byteBuf.getLongLE(calculatedIndex);
    } else if (order() == ByteOrder.BIG_ENDIAN) {
      return (_getInt(index) & 0xffffffffL) << 32 | _getInt(index + 4) & 0xffffffffL;
    } else {
      return _getInt(index) & 0xFFFFFFFFL | (_getInt(index + 4) & 0xFFFFFFFFL) << 32;
    }
  }

  @Override
  public ByteBufAllocator alloc() {
    return allocator;
  }

  @Override
  public int capacity() {
    return capacity;
  }

  @Override
  public ByteBuf capacity(int newCapacity) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int maxCapacity() {
    return capacity;
  }

  @Override
  public ByteOrder order() {
    return ByteOrder.LITTLE_ENDIAN;
  }

  @Override
  public ByteBuf order(ByteOrder endianness) {
    return this;
  }

  @Override
  public ByteBuf unwrap() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isReadOnly() {
    return true;
  }

  @Override
  public ByteBuf asReadOnly() {
    return this;
  }

  @Override
  public boolean isWritable() {
    return false;
  }

  @Override
  public boolean isWritable(int size) {
    return false;
  }

  @Override
  public ByteBuf writerIndex(int writerIndex) {
    return this;
  }

  @Override
  public final int writerIndex() {
    return capacity;
  }

  @Override
  public ByteBuf setIndex(int readerIndex, int writerIndex) {
    return this;
  }

  @Override
  public ByteBuf clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf discardReadBytes() {
    return this;
  }

  @Override
  public ByteBuf discardSomeReadBytes() {
    return this;
  }

  @Override
  public ByteBuf ensureWritable(int minWritableBytes) {
    return this;
  }

  @Override
  public int ensureWritable(int minWritableBytes, boolean force) {
    return 0;
  }

  @Override
  public ByteBuf setFloatLE(int index, float value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setDoubleLE(int index, double value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setBoolean(int index, boolean value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setByte(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setShort(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setShortLE(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setMedium(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setMediumLE(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setInt(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setIntLE(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setLong(int index, long value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setLongLE(int index, long value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setChar(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setFloat(int index, float value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setDouble(int index, double value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuf src) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuf src, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setBytes(int index, byte[] src) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuffer src) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int setBytes(int index, InputStream in, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int setBytes(int index, ScatteringByteChannel in, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int setBytes(int index, FileChannel in, long position, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int setCharSequence(int index, CharSequence sequence, Charset charset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setZero(int index, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeBoolean(boolean value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeByte(int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeShort(int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeShortLE(int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeMedium(int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeMediumLE(int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeInt(int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeIntLE(int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeLong(long value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeLongLE(long value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeChar(int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeFloat(float value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeDouble(double value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeBytes(ByteBuf src) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeBytes(ByteBuf src, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeBytes(ByteBuf src, int srcIndex, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeBytes(byte[] src) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeBytes(byte[] src, int srcIndex, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeBytes(ByteBuffer src) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int writeBytes(InputStream in, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int writeBytes(ScatteringByteChannel in, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int writeBytes(FileChannel in, long position, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeZero(int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int writeCharSequence(CharSequence sequence, Charset charset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuffer internalNioBuffer(int index, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasArray() {
    return false;
  }

  @Override
  public byte[] array() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int arrayOffset() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasMemoryAddress() {
    return false;
  }

  @Override
  public long memoryAddress() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void _setByte(int index, int value) {}

  @Override
  protected void _setShort(int index, int value) {}

  @Override
  protected void _setShortLE(int index, int value) {}

  @Override
  protected void _setMedium(int index, int value) {}

  @Override
  protected void _setMediumLE(int index, int value) {}

  @Override
  protected void _setInt(int index, int value) {}

  @Override
  protected void _setIntLE(int index, int value) {}

  @Override
  protected void _setLong(int index, long value) {}

  @Override
  protected void _setLongLE(int index, long value) {}
}
