/**
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket.netty;

import io.netty.buffer.ByteBuf;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;

public class MutableDirectByteBuf implements MutableDirectBuffer
{
    private ByteBuf byteBuf;

    public MutableDirectByteBuf(final ByteBuf byteBuf)
    {
        this.byteBuf = byteBuf;
    }

    public void wrap(final ByteBuf byteBuf)
    {
        this.byteBuf = byteBuf;
    }

    public ByteBuf byteBuf()
    {
        return byteBuf;
    }

    // TODO: make utility in reactivesocket-java
    public static ByteBuffer slice(final ByteBuffer byteBuffer, final int position, final int limit)
    {
        final int savedPosition = byteBuffer.position();
        final int savedLimit = byteBuffer.limit();

        byteBuffer.limit(limit).position(position);

        final ByteBuffer result = byteBuffer.slice();

        byteBuffer.limit(savedLimit).position(savedPosition);
        return result;
    }

    @Override
    public void setMemory(int index, int length, byte value)
    {
        for (int i = index; i < (index + length); i++)
        {
            byteBuf.setByte(i, value);
        }
    }

    @Override
    public void putLong(int index, long value, ByteOrder byteOrder)
    {
        ensureByteOrder(byteOrder);
        byteBuf.setLong(index, value);
    }

    @Override
    public void putLong(int index, long value)
    {
        byteBuf.setLong(index, value);
    }

    @Override
    public void putInt(int index, int value, ByteOrder byteOrder)
    {
        ensureByteOrder(byteOrder);
        byteBuf.setInt(index, value);
    }

    @Override
    public void putInt(int index, int value)
    {
        byteBuf.setInt(index, value);
    }

    @Override
    public void putDouble(int index, double value, ByteOrder byteOrder)
    {
        ensureByteOrder(byteOrder);
        byteBuf.setDouble(index, value);
    }

    @Override
    public void putDouble(int index, double value)
    {
        byteBuf.setDouble(index, value);
    }

    @Override
    public void putFloat(int index, float value, ByteOrder byteOrder)
    {
        ensureByteOrder(byteOrder);
        byteBuf.setFloat(index, value);
    }

    @Override
    public void putFloat(int index, float value)
    {
        byteBuf.setFloat(index, value);
    }

    @Override
    public void putShort(int index, short value, ByteOrder byteOrder)
    {
        ensureByteOrder(byteOrder);
        byteBuf.setShort(index, value);
    }

    @Override
    public void putShort(int index, short value)
    {
        byteBuf.setShort(index, value);
    }

    @Override
    public void putByte(int index, byte value)
    {
        byteBuf.setByte(index, value);
    }

    @Override
    public void putBytes(int index, byte[] src)
    {
        byteBuf.setBytes(index, src);
    }

    @Override
    public void putBytes(int index, byte[] src, int offset, int length)
    {
        byteBuf.setBytes(index, src, offset, length);
    }

    @Override
    public void putBytes(int index, ByteBuffer srcBuffer, int length)
    {
        final ByteBuffer sliceBuffer = slice(srcBuffer, 0, length);
        byteBuf.setBytes(index, sliceBuffer);
    }

    @Override
    public void putBytes(int index, ByteBuffer srcBuffer, int srcIndex, int length)
    {
        final ByteBuffer sliceBuffer = slice(srcBuffer, srcIndex, srcIndex + length);
        byteBuf.setBytes(index, sliceBuffer);
    }

    @Override
    public void putBytes(int index, DirectBuffer srcBuffer, int srcIndex, int length)
    {
        throw new UnsupportedOperationException("putBytes(DirectBuffer) not supported");
    }

    @Override
    public int putStringUtf8(int offset, String value, ByteOrder byteOrder)
    {
        throw new UnsupportedOperationException("putStringUtf8 not supported");
    }

    @Override
    public int putStringUtf8(int offset, String value, ByteOrder byteOrder, int maxEncodedSize)
    {
        throw new UnsupportedOperationException("putStringUtf8 not supported");
    }

    @Override
    public int putStringWithoutLengthUtf8(int offset, String value)
    {
        throw new UnsupportedOperationException("putStringUtf8 not supported");
    }

    @Override
    public void wrap(byte[] buffer)
    {
        throw new UnsupportedOperationException("wrap(byte[]) not supported");
    }

    @Override
    public void wrap(byte[] buffer, int offset, int length)
    {
        throw new UnsupportedOperationException("wrap(byte[]) not supported");
    }

    @Override
    public void wrap(ByteBuffer buffer)
    {
        throw new UnsupportedOperationException("wrap(ByteBuffer) not supported");
    }

    @Override
    public void wrap(ByteBuffer buffer, int offset, int length)
    {
        throw new UnsupportedOperationException("wrap(ByteBuffer) not supported");
    }

    @Override
    public void wrap(DirectBuffer buffer)
    {
        throw new UnsupportedOperationException("wrap(DirectBuffer) not supported");
    }

    @Override
    public void wrap(DirectBuffer buffer, int offset, int length)
    {
        throw new UnsupportedOperationException("wrap(DirectBuffer) not supported");
    }

    @Override
    public void wrap(long address, int length)
    {
        throw new UnsupportedOperationException("wrap(address) not supported");
    }

    @Override
    public long addressOffset()
    {
        return byteBuf.memoryAddress();
    }

    @Override
    public byte[] byteArray()
    {
        return byteBuf.array();
    }

    @Override
    public ByteBuffer byteBuffer()
    {
        return byteBuf.nioBuffer();
    }

    @Override
    public int capacity()
    {
        return byteBuf.capacity();
    }

    @Override
    public void checkLimit(int limit)
    {
        throw new UnsupportedOperationException("checkLimit not supported");
    }

    @Override
    public long getLong(int index, ByteOrder byteOrder)
    {
        ensureByteOrder(byteOrder);
        return byteBuf.getLong(index);
    }

    @Override
    public long getLong(int index)
    {
        return byteBuf.getLong(index);
    }

    @Override
    public int getInt(int index, ByteOrder byteOrder)
    {
        ensureByteOrder(byteOrder);
        return byteBuf.getInt(index);
    }

    @Override
    public int getInt(int index)
    {
        return byteBuf.getInt(index);
    }

    @Override
    public double getDouble(int index, ByteOrder byteOrder)
    {
        ensureByteOrder(byteOrder);
        return byteBuf.getDouble(index);
    }

    @Override
    public double getDouble(int index)
    {
        return byteBuf.getDouble(index);
    }

    @Override
    public float getFloat(int index, ByteOrder byteOrder)
    {
        ensureByteOrder(byteOrder);
        return byteBuf.getFloat(index);
    }

    @Override
    public float getFloat(int index)
    {
        return byteBuf.getFloat(index);
    }

    @Override
    public short getShort(int index, ByteOrder byteOrder)
    {
        ensureByteOrder(byteOrder);
        return byteBuf.getShort(index);
    }

    @Override
    public short getShort(int index)
    {
        return byteBuf.getShort(index);
    }

    @Override
    public byte getByte(int index)
    {
        return byteBuf.getByte(index);
    }

    @Override
    public void getBytes(int index, byte[] dst)
    {
        byteBuf.getBytes(index, dst);
    }

    @Override
    public void getBytes(int index, byte[] dst, int offset, int length)
    {
        byteBuf.getBytes(index, dst, offset, length);
    }

    @Override
    public void getBytes(int index, MutableDirectBuffer dstBuffer, int dstIndex, int length)
    {
        throw new UnsupportedOperationException("getBytes(MutableDirectBuffer) not supported");
    }

    @Override
    public void getBytes(int index, ByteBuffer dstBuffer, int length)
    {
        throw new UnsupportedOperationException("getBytes(ByteBuffer) not supported");
    }

    @Override
    public String getStringUtf8(int offset, ByteOrder byteOrder)
    {
        final int length = getInt(offset, byteOrder);
        return byteBuf.toString(offset + BitUtil.SIZE_OF_INT, length, Charset.forName("UTF-8"));
    }

    @Override
    public String getStringUtf8(int offset, int length)
    {
        return byteBuf.toString(offset, length, Charset.forName("UTF-8"));
    }

    @Override
    public String getStringWithoutLengthUtf8(int offset, int length)
    {
        return byteBuf.toString(offset, length, Charset.forName("UTF-8"));
    }

    @Override
    public void boundsCheck(int index, int length)
    {
        throw new UnsupportedOperationException("boundsCheck not supported");
    }

    private void ensureByteOrder(final ByteOrder byteOrder)
    {
        if (byteBuf.order() != byteOrder)
        {
            byteBuf.order(byteOrder);
        }
    }
}
