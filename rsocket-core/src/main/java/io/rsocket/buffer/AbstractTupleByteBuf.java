package io.rsocket.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.internal.SystemPropertyUtil;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public abstract class AbstractTupleByteBuf extends AbstractReadOnlyReferenceCountedByteBuf {
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
      return buffers[0];
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
      return _getIntLE(index) & 0xffffffffL | (_getIntLE(index + 4) & 0xffffffffL) << 32;
    } else {
      return (_getIntLE(index) & 0xffffffffL) << 32 | _getIntLE(index + 4) & 0xffffffffL;
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
  public int maxCapacity() {
    return capacity;
  }

  @Override
  public ByteOrder order() {
    return ByteOrder.BIG_ENDIAN;
  }

  @Override
  public ByteBuf unwrap() {
    return null;
  }

  @Override
  public boolean isReadOnly() {
    return true;
  }

  @Override
  public AbstractTupleByteBuf asReadOnly() {
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
  public AbstractTupleByteBuf readerIndex(int readerIndex) {
    super.readerIndex(readerIndex);
    return this;
  }

  @Override
  public AbstractTupleByteBuf writerIndex(int writerIndex) {
    super.writerIndex(writerIndex);
    return this;
  }

  @Override
  public AbstractTupleByteBuf setIndex(int readerIndex, int writerIndex) {
    super.setIndex(readerIndex, writerIndex);
    return this;
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

  protected ByteBuf allocBuffer(int capacity) {
    return isDirect() ? alloc().directBuffer(capacity) : alloc().heapBuffer(capacity);
  }
}
