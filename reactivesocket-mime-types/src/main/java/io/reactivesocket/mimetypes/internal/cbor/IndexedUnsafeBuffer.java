/*
 * Copyright 2016 Netflix, Inc.
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
 *
 */

package io.reactivesocket.mimetypes.internal.cbor;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Function;

public class IndexedUnsafeBuffer {

    public static final byte[] EMPTY_ARRAY = new byte[0];

    private int readerIndex;
    private int writerIndex;
    private int backingBufferOffset;
    private final UnsafeBuffer delegate;
    private final ByteOrder byteOrder;

    public IndexedUnsafeBuffer(ByteOrder byteOrder) {
        this.byteOrder = byteOrder;
        delegate = new UnsafeBuffer(EMPTY_ARRAY);
    }

    public void wrap(ByteBuffer buffer) {
        wrap(buffer, 0, buffer.capacity());
    }

    public void wrap(ByteBuffer buffer, int offset, int length) {
        delegate.wrap(buffer, offset, length);
        readerIndex = 0;
        writerIndex = 0;
        backingBufferOffset = offset;
    }

    public void wrap(DirectBuffer buffer) {
        wrap(buffer, 0, buffer.capacity());
    }

    public void wrap(DirectBuffer buffer, int offset, int length) {
        delegate.wrap(buffer, offset, length);
        readerIndex = 0;
        writerIndex = 0;
        backingBufferOffset = offset;
    }

    public short readUnsignedByte() {
        return  (short) (delegate.getByte(readerIndex++) & 0xff);
    }

    public byte readByte() {
        return delegate.getByte(readerIndex++);
    }

    public int readShort() {
        short s = delegate.getShort(readerIndex, byteOrder);
        readerIndex += BitUtil.SIZE_OF_SHORT;
        return s;
    }

    public int readInt() {
        int i = delegate.getInt(readerIndex, byteOrder);
        readerIndex += BitUtil.SIZE_OF_INT;
        return i;
    }

    public long readLong() {
        long l = delegate.getLong(readerIndex, byteOrder);
        readerIndex += BitUtil.SIZE_OF_LONG;
        return l;
    }

    public void readBytes(byte[] dst, int dstOffset, int length) {
        delegate.getBytes(readerIndex, dst, dstOffset, length);
        readerIndex += length - dstOffset;
    }

    public void readBytes(MutableDirectBuffer dst, int offset, int length) {
        delegate.getBytes(readerIndex, dst, offset, length);
        readerIndex += length;
    }

    public void readBytes(IndexedUnsafeBuffer dst, int length) {
        delegate.getBytes(readerIndex, dst.getBackingBuffer(), dst.getWriterIndex(), length);
        readerIndex += length;
    }

    public void writeByte(byte toWrite) {
        delegate.putByte(writerIndex++, toWrite);
    }

    public void writeShort(short toWrite) {
        delegate.putShort(writerIndex, toWrite, byteOrder);
        writerIndex += BitUtil.SIZE_OF_SHORT;
    }

    public void writeInt(int toWrite) {
        delegate.putInt(writerIndex, toWrite, byteOrder);
        writerIndex += BitUtil.SIZE_OF_INT;
    }

    public void writeLong(long toWrite) {
        delegate.putLong(writerIndex, toWrite, byteOrder);
        writerIndex += BitUtil.SIZE_OF_LONG;
    }

    public void writeBytes(byte[] src, int offset, int length) {
        delegate.putBytes(writerIndex, src, offset, length);
        writerIndex += length;
    }

    public void writeBytes(ByteBuffer src, int length) {
        delegate.putBytes(writerIndex, src, src.position(), length);
        writerIndex += length;
    }

    public void writeBytes(DirectBuffer src, int offset, int length) {
        delegate.putBytes(writerIndex, src, offset, length);
        writerIndex += length;
    }

    public int getReaderIndex() {
        return readerIndex;
    }

    public int getWriterIndex() {
        return writerIndex;
    }

    public UnsafeBuffer getBackingBuffer() {
        return delegate;
    }

    public int getBackingBufferOffset() {
        return backingBufferOffset;
    }

    public void incrementReaderIndex(int increment) {
        readerIndex += increment;
    }

    public void setReaderIndex(int readerIndex) {
        if (readerIndex >= delegate.capacity()) {
            throw new IllegalArgumentException(
                    String.format("Reader Index should be less than capacity. Reader Index: %d, Capacity: %d",
                                  readerIndex, delegate.capacity()));
        }
        this.readerIndex = readerIndex;
    }

    public void setWriterIndex(int writerIndex) {
        if (writerIndex >= delegate.capacity()) {
            throw new IllegalArgumentException(
                    String.format("Writer Index should be less than capacity. Writer Index: %d, Capacity: %d",
                                  writerIndex, delegate.capacity()));
        }
        this.writerIndex = writerIndex;
    }

    public void incrementWriterIndex(int increment) {
        writerIndex += increment;
    }

    /**
     * Scans this buffer and invokes the passed {@code scanner} for every byte. The scan stops if it has reached the end
     * of buffer or the {@code scanner} returns {@code false}. This method does not move the {@code readerIndex} for
     * this buffer.
     *
     * @param scanner Scanner that determines to scan further for every byte.
     *
     * @return Index in the buffer at which this scan stopped.
     */
    public int forEachByte(Function<Byte, Boolean> scanner) {
        int i;
        for (i = readerIndex; i < delegate.capacity(); i++) {
            if (!scanner.apply(delegate.getByte(i))) {
                break;
            }
        }
        return i;
    }
}
