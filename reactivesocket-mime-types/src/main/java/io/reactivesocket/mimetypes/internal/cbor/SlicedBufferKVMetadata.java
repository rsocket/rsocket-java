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

import io.reactivesocket.mimetypes.KVMetadata;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * An implementation of {@link KVMetadata} that does not allocate buffers for values of metadata, instead it keeps a
 * view of the original buffer with offsets to the values. See {@link CBORMap} to learn more about the structure and
 * cases when this creates allocations.
 *
 * <h2>Lifecycle</h2>
 *
 * This assumes exclusive access to the underlying buffer. If that is not the case, then {@link #duplicate(Function)}
 * must be used to create a copy of the underlying buffer.
 *
 * @see CBORMap
 */
public class SlicedBufferKVMetadata extends CBORMap implements KVMetadata {

    public SlicedBufferKVMetadata(DirectBuffer backingBuffer, int offset, int initialCapacity) {
        super(backingBuffer, offset, initialCapacity);
    }

    public SlicedBufferKVMetadata(DirectBuffer backingBuffer, int offset) {
        super(backingBuffer, offset);
    }

    protected SlicedBufferKVMetadata(Map<String, ByteBuffer> storeWhenModified) {
        super(storeWhenModified);
    }

    protected SlicedBufferKVMetadata(DirectBuffer backingBuffer, int offset, Map<String, Long> keysVsOffsets) {
        super(backingBuffer, offset, keysVsOffsets);
    }

    @Override
    public String getAsString(String key, Charset valueEncoding) {
        ByteBuffer v = get(key);
        byte[] vBytes;
        if (v.hasArray()) {
            return new String(v.array(), v.arrayOffset(), v.limit(), valueEncoding);
        } else {
            vBytes = new byte[v.remaining()];
            v.get(vBytes);
            return new String(vBytes, valueEncoding);
        }
    }

    @Override
    public KVMetadata duplicate(Function<Integer, MutableDirectBuffer> newBufferFactory) {
        if (null == storeWhenModified) {
            int newCap = backingBuffer.capacity();
            MutableDirectBuffer newBuffer = newBufferFactory.apply(newCap);
            backingBuffer.getBytes(0, newBuffer, 0, newCap);
            return new SlicedBufferKVMetadata(newBuffer, 0, new HashMap<>(keysVsOffsets));
        } else {
            return new SlicedBufferKVMetadata(storeWhenModified);
        }
    }
}
