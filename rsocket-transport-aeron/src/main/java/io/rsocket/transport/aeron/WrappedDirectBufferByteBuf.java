/*
 * Copyright 2015-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rsocket.transport.aeron;

import io.netty.buffer.AbstractByteBuf;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import org.agrona.DirectBuffer;

class WrappedDirectBufferByteBuf extends AbstractByteBuf {

  private DirectBuffer directBuffer;
  private final ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;

  public WrappedDirectBufferByteBuf() {
    super(Integer.MAX_VALUE);
  }

  public void wrap(DirectBuffer c, int offset, int limit) {
    this.directBuffer = c;
    this.setIndex(offset, limit);
  }

  @Override
  protected byte _getByte(int index) {
    return directBuffer.getByte(index);
  }

  @Override
  protected short _getShort(int index) {
    return directBuffer.getShort(index, byteOrder);
  }

  @Override
  protected short _getShortLE(int index) {
    return directBuffer.getShort(index, ByteOrder.LITTLE_ENDIAN);
  }

  @Override
  protected int _getUnsignedMedium(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected int _getUnsignedMediumLE(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected int _getInt(int index) {
    return directBuffer.getInt(index, byteOrder);
  }

  @Override
  protected int _getIntLE(int index) {
    return directBuffer.getInt(index, ByteOrder.LITTLE_ENDIAN);
  }

  @Override
  protected long _getLong(int index) {
    return directBuffer.getLong(index, byteOrder);
  }

  @Override
  protected long _getLongLE(int index) {
    return directBuffer.getLong(index, ByteOrder.LITTLE_ENDIAN);
  }

  @Override
  protected void _setByte(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void _setShort(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void _setShortLE(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void _setMedium(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void _setMediumLE(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void _setInt(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void _setIntLE(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void _setLong(int index, long value) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void _setLongLE(int index, long value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int capacity() {
    return directBuffer.capacity();
  }

  @Override
  public ByteBuf capacity(int newCapacity) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBufAllocator alloc() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteOrder order() {
    return ByteOrder.nativeOrder();
  }

  @Override
  public ByteBuf unwrap() {
    return null;
  }

  @Override
  public boolean isDirect() {
    return directBuffer.byteBuffer().isDirect();
  }

  @Override
  public boolean isReadOnly() {
    return true;
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
    directBuffer.getBytes(index, dst, dstIndex, length);
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuffer dst) {
    directBuffer.getBytes(index, dst, dst.remaining());
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, OutputStream out, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getBytes(int index, GatheringByteChannel out, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getBytes(int index, FileChannel out, long position, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
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
  public ByteBuf copy(int index, int length) {
    checkIndex(index, length);
    return alloc()
        .heapBuffer(length, maxCapacity())
        .writeBytes(directBuffer.byteArray(), index, length);
  }

  @Override
  public int nioBufferCount() {
    return 1;
  }

  @Override
  public ByteBuffer nioBuffer(int index, int length) {
    final ByteBuffer buffer = directBuffer.byteBuffer();
    if (buffer != null) {
      return (ByteBuffer) buffer.duplicate().position(index).limit(index + length);
    } else {
      final byte[] bytes = directBuffer.byteArray();
      return ByteBuffer.wrap(bytes, index, length);
    }
  }

  @Override
  public ByteBuffer internalNioBuffer(int index, int length) {
    return nioBuffer(index, length);
  }

  @Override
  public ByteBuffer[] nioBuffers(int index, int length) {
    return new ByteBuffer[] {nioBuffer(index, length)};
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
    return true;
  }

  @Override
  public long memoryAddress() {
    return directBuffer.addressOffset();
  }

  @Override
  public int refCnt() {
    return 1;
  }

  @Override
  public ByteBuf touch() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf touch(Object hint) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf retain(int increment) {
    return this;
  }

  @Override
  public ByteBuf retain() {
    return this;
  }

  @Override
  public boolean release() {
    return false;
  }

  @Override
  public boolean release(int decrement) {
    return false;
  }
}
