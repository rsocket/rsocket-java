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
package io.reactivesocket.internal.frame;

import io.reactivesocket.Frame;
import io.reactivesocket.Payload;
import org.agrona.BitUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Builder for appending buffers that grows dataCapacity as necessary. Similar to Aeron's PayloadBuilder.
 */
public class PayloadBuilder
{
    public static final int INITIAL_CAPACITY = Math.max(Frame.DATA_MTU, Frame.METADATA_MTU);

    private final MutableDirectBuffer dataMutableDirectBuffer;
    private final MutableDirectBuffer metadataMutableDirectBuffer;

    private byte[] dataBuffer;
    private byte[] metadataBuffer;
    private int dataLimit = 0;
    private int metadataLimit = 0;
    private int dataCapacity;
    private int metadataCapacity;

    public PayloadBuilder()
    {
        dataCapacity = BitUtil.findNextPositivePowerOfTwo(INITIAL_CAPACITY);
        metadataCapacity = BitUtil.findNextPositivePowerOfTwo(INITIAL_CAPACITY);
        dataBuffer = new byte[dataCapacity];
        metadataBuffer = new byte[metadataCapacity];
        dataMutableDirectBuffer = new UnsafeBuffer(dataBuffer);
        metadataMutableDirectBuffer = new UnsafeBuffer(metadataBuffer);
    }

    public Payload payload()
    {
        return new Payload()
        {
            public ByteBuffer getData()
            {
                return ByteBuffer.wrap(dataBuffer, 0, dataLimit);
            }

            public ByteBuffer getMetadata()
            {
                return ByteBuffer.wrap(metadataBuffer, 0, metadataLimit);
            }
        };
    }

    public void append(final Payload payload)
    {
        final ByteBuffer payloadData = payload.getData();
        final ByteBuffer payloadMetadata = payload.getMetadata();
        final int dataLength = payloadData.remaining();
        final int metadataLength = payloadMetadata.remaining();

        ensureDataCapacity(dataLength);
        ensureMetadataCapacity(metadataLength);

        dataMutableDirectBuffer.putBytes(dataLimit, payloadData, payloadData.capacity());
        dataLimit += dataLength;
        metadataMutableDirectBuffer.putBytes(metadataLimit, payloadMetadata, payloadMetadata.capacity());
        metadataLimit += metadataLength;
    }

    private void ensureDataCapacity(final int additionalCapacity)
    {
        final int requiredCapacity = dataLimit + additionalCapacity;

        if (requiredCapacity < 0)
        {
            final String s = String.format("Insufficient data capacity: dataLimit=%d additional=%d", dataLimit, additionalCapacity);
            throw new IllegalStateException(s);
        }

        if (requiredCapacity > dataCapacity)
        {
            final int newCapacity = findSuitableCapacity(dataCapacity, requiredCapacity);
            final byte[] newBuffer = Arrays.copyOf(dataBuffer, newCapacity);

            dataCapacity = newCapacity;
            dataBuffer = newBuffer;
            dataMutableDirectBuffer.wrap(newBuffer);
        }
    }

    private void ensureMetadataCapacity(final int additionalCapacity)
    {
        final int requiredCapacity = metadataLimit + additionalCapacity;

        if (requiredCapacity < 0)
        {
            final String s = String.format("Insufficient metadata capacity: metadataLimit=%d additional=%d", metadataLimit, additionalCapacity);
            throw new IllegalStateException(s);
        }

        if (requiredCapacity > metadataCapacity)
        {
            final int newCapacity = findSuitableCapacity(metadataCapacity, requiredCapacity);
            final byte[] newBuffer = Arrays.copyOf(metadataBuffer, newCapacity);

            metadataCapacity = newCapacity;
            metadataBuffer = newBuffer;
            metadataMutableDirectBuffer.wrap(newBuffer);
        }
    }

    private static int findSuitableCapacity(int capacity, final int requiredCapacity)
    {
        do
        {
            capacity <<= 1;
        }
        while (capacity < requiredCapacity);

        return capacity;
    }
}
