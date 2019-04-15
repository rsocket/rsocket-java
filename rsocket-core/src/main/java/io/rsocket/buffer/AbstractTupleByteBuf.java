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
  protected byte _getByte(int index) {
    long ri = calculateRelativeIndex(index);
    ByteBuf byteBuf = getPart(index);

    index = (int) (ri & Integer.MAX_VALUE);

    return byteBuf.getByte(index);
  }

  @Override
  protected short _getShort(int index) {
    long ri = calculateRelativeIndex(index);
    ByteBuf byteBuf = getPart(index);

    index = (int) (ri & Integer.MAX_VALUE);

    return byteBuf.getShort(index);
  }

  @Override
  protected short _getShortLE(int index) {
    long ri = calculateRelativeIndex(index);
    ByteBuf byteBuf = getPart(index);

    index = (int) (ri & Integer.MAX_VALUE);

    return byteBuf.getShortLE(index);
  }

  @Override
  protected int _getUnsignedMedium(int index) {
    long ri = calculateRelativeIndex(index);
    ByteBuf byteBuf = getPart(index);

    index = (int) (ri & Integer.MAX_VALUE);

    return byteBuf.getUnsignedMedium(index);
  }

  @Override
  protected int _getUnsignedMediumLE(int index) {
    long ri = calculateRelativeIndex(index);
    ByteBuf byteBuf = getPart(index);

    index = (int) (ri & Integer.MAX_VALUE);

    return byteBuf.getUnsignedMediumLE(index);
  }

  @Override
  protected int _getInt(int index) {
    long ri = calculateRelativeIndex(index);
    ByteBuf byteBuf = getPart(index);

    index = (int) (ri & Integer.MAX_VALUE);

    return byteBuf.getInt(index);
  }

  @Override
  protected int _getIntLE(int index) {
    long ri = calculateRelativeIndex(index);
    ByteBuf byteBuf = getPart(index);

    index = (int) (ri & Integer.MAX_VALUE);

    return byteBuf.getIntLE(index);
  }

  @Override
  protected long _getLong(int index) {
    long ri = calculateRelativeIndex(index);
    ByteBuf byteBuf = getPart(index);

    index = (int) (ri & Integer.MAX_VALUE);

    return byteBuf.getLong(index);
  }

  @Override
  protected long _getLongLE(int index) {
    long ri = calculateRelativeIndex(index);
    ByteBuf byteBuf = getPart(index);

    index = (int) (ri & Integer.MAX_VALUE);

    return byteBuf.getLongLE(index);
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
  public int compareTo(ByteBuf buffer) {
    return 0;
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
