package io.rsocket.buffer;

import io.netty.buffer.AbstractReferenceCountedByteBuf;
import io.netty.buffer.ByteBuf;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.nio.channels.FileChannel;
import java.nio.channels.ScatteringByteChannel;

abstract class AbstractReadOnlyReferenceCountedByteBuf extends AbstractReferenceCountedByteBuf {

  AbstractReadOnlyReferenceCountedByteBuf(int maxCapacity) {
    super(maxCapacity);
  }

  @Override
  public boolean isReadOnly() {
    return true;
  }

  @Override
  public boolean isWritable() {
    return false;
  }

  @Override
  public boolean isWritable(int numBytes) {
    return false;
  }

  @Override
  public int ensureWritable(int minWritableBytes, boolean force) {
    return 1;
  }

  @Override
  public ByteBuf ensureWritable(int minWritableBytes) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public ByteBuf discardReadBytes() {
    throw new ReadOnlyBufferException();
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuffer src) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public ByteBuf setByte(int index, int value) {
    throw new ReadOnlyBufferException();
  }

  @Override
  protected void _setByte(int index, int value) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public ByteBuf setShort(int index, int value) {
    throw new ReadOnlyBufferException();
  }

  @Override
  protected void _setShort(int index, int value) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public ByteBuf setShortLE(int index, int value) {
    throw new ReadOnlyBufferException();
  }

  @Override
  protected void _setShortLE(int index, int value) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public ByteBuf setMedium(int index, int value) {
    throw new ReadOnlyBufferException();
  }

  @Override
  protected void _setMedium(int index, int value) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public ByteBuf setMediumLE(int index, int value) {
    throw new ReadOnlyBufferException();
  }

  @Override
  protected void _setMediumLE(int index, int value) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public ByteBuf setInt(int index, int value) {
    throw new ReadOnlyBufferException();
  }

  @Override
  protected void _setInt(int index, int value) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public ByteBuf setIntLE(int index, int value) {
    throw new ReadOnlyBufferException();
  }

  @Override
  protected void _setIntLE(int index, int value) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public ByteBuf setLong(int index, long value) {
    throw new ReadOnlyBufferException();
  }

  @Override
  protected void _setLong(int index, long value) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public ByteBuf setLongLE(int index, long value) {
    throw new ReadOnlyBufferException();
  }

  @Override
  protected void _setLongLE(int index, long value) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public int setBytes(int index, InputStream in, int length) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public int setBytes(int index, ScatteringByteChannel in, int length) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public int setBytes(int index, FileChannel in, long position, int length) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public ByteBuf capacity(int newCapacity) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public ByteBuf asReadOnly() {
    return this;
  }
}
