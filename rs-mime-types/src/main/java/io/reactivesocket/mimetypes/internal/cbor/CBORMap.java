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

import io.reactivesocket.internal.frame.ByteBufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.reactivesocket.mimetypes.internal.cbor.CborMajorType.*;

/**
 * A representation of CBOR map as defined in the <a href="https://tools.ietf.org/html/rfc7049#page-7">spec</a>. <p>
 *
 * The benefit of this class is that it does not create additional buffers for the values of the metadata, when
 * possible. Instead it holds views into the original buffer and when queried creates a slice of the underlying buffer
 * that represents the value. Thus, it is lean in terms of memory usage as compared to other standard libraries that
 * allocate memory for all values.
 *
 * <h1>Allocations</h1>
 *
 * <h2>Modifications</h2>
 *
 * Any additions to the map (adding one or more key-value pairs) will create a map with all keys and values, where
 * values are the buffer slices of the original buffer. From then onwards, the newly created map will be used for all
 * further queries. <p>
 *     So, additions to this map will result in more allocations than usual but it still does not allocate fresh memory
 *     for existing entries.
 *
 * <h2>Access</h2>
 *
 * Bulk queries like {@link #entrySet()}, {@link #values()} and value queries like {@link #containsValue(Object)} will
 * switch to a new map as described in case of modifications above.
 *
 * <h1>Structure</h1>
 *
 * In absence of the above cases for allocations, this map uses an index of {@code String} keys to a {@code Long}. The
 * first 32 bits of this {@code Long} holds the length of the value and the next 32 bits contain the offset in the
 * original buffer. {@link #encodeValueMask(int, long)} encodes this mask and {@link #decodeLengthFromMask(long)},
 * {@link #decodeOffsetFromMask(long)} decodes the mask.
 */
public class CBORMap implements Map<String, ByteBuffer> {

    protected final DirectBuffer backingBuffer;
    protected final int offset;
    protected final Map<String, Long> keysVsOffsets;
    protected Map<String, ByteBuffer> storeWhenModified;

    public CBORMap(DirectBuffer backingBuffer, int offset) {
        this(backingBuffer, offset, 16, 0.75f);
    }

    public CBORMap(DirectBuffer backingBuffer, int offset, int initialCapacity) {
        this(backingBuffer, offset, initialCapacity, 0.75f);
    }

    public CBORMap(DirectBuffer backingBuffer, int offset, int initialCapacity, float loadFactor) {
        this.backingBuffer = backingBuffer;
        this.offset = offset;
        keysVsOffsets = new HashMap<>(initialCapacity, loadFactor);
    }

    protected CBORMap(Map<String, ByteBuffer> storeWhenModified) {
        backingBuffer = new UnsafeBuffer(IndexedUnsafeBuffer.EMPTY_ARRAY);
        offset = 0;
        this.storeWhenModified = storeWhenModified;
        keysVsOffsets = Collections.emptyMap();
    }

    protected CBORMap(DirectBuffer backingBuffer, int offset, Map<String, Long> keysVsOffsets) {
        this.backingBuffer = backingBuffer;
        this.offset = offset;
        this.keysVsOffsets = keysVsOffsets;
        storeWhenModified = null;
    }

    public Long putValueOffset(String key, int offset, int length) {
        return keysVsOffsets.put(key, encodeValueMask(offset, length));
    }

    @Override
    public int size() {
        return null != storeWhenModified ? storeWhenModified.size() : keysVsOffsets.size();
    }

    @Override
    public boolean isEmpty() {
        return null != storeWhenModified ? storeWhenModified.isEmpty() : keysVsOffsets.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return null != storeWhenModified ? storeWhenModified.containsKey(key) : keysVsOffsets.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        switchToAlternateMap();
        return storeWhenModified.containsValue(value);
    }

    @Override
    public ByteBuffer get(Object key) {
        return getByteBuffer((String) key);
    }

    @Override
    public ByteBuffer put(String key, ByteBuffer value) {
        if (null != storeWhenModified) {
            return storeWhenModified.put(key, value);
        }

        switchToAlternateMap();
        return storeWhenModified.put(key, value);
    }

    @Override
    public ByteBuffer remove(Object key) {
        if (null != storeWhenModified) {
            return storeWhenModified.remove(key);
        }

        Long removed = keysVsOffsets.remove(key);
        return getFromBackingBuffer(removed);
    }

    @Override
    public void putAll(Map<? extends String, ? extends ByteBuffer> m) {
        if (null != storeWhenModified) {
            storeWhenModified.putAll(m);
        } else {
            switchToAlternateMap();
            storeWhenModified.putAll(m);
        }
    }

    @Override
    public void clear() {
        if (null != storeWhenModified) {
            storeWhenModified.clear();
        } else {
            keysVsOffsets.clear();
        }
    }

    @Override
    public Set<String> keySet() {
        return null != storeWhenModified ? storeWhenModified.keySet() : keysVsOffsets.keySet();
    }

    @Override
    public Collection<ByteBuffer> values() {
        switchToAlternateMap();
        return storeWhenModified.values();
    }

    @Override
    public Set<Entry<String, ByteBuffer>> entrySet() {
        switchToAlternateMap();
        return storeWhenModified.entrySet();
    }

    public void encodeTo(IndexedUnsafeBuffer dst) {
        if (null == storeWhenModified) {
            final int size = keysVsOffsets.size();
            CborHeader.forLengthToEncode(size).encode(dst, MAP, size);
            for (Entry<String, Long> entry : keysVsOffsets.entrySet()) {
                CborUtf8StringCodec.encode(dst, entry.getKey());

                Long valueMask = entry.getValue();
                int valueLength = decodeLengthFromMask(valueMask);
                int valueOffset = decodeOffsetFromMask(valueMask);
                CborBinaryStringCodec.encode(dst, backingBuffer, valueOffset, valueLength);
            }
        } else {
            CborMapCodec.encode(dst, storeWhenModified);
        }
    }

    DirectBuffer getBackingBuffer() {
        return backingBuffer;
    }

    private ByteBuffer getByteBuffer(String key) {
        if (null == storeWhenModified) {
            Long valueMask = keysVsOffsets.get(key);
            return null == valueMask ? null : getFromBackingBuffer(valueMask);
        } else {
            return storeWhenModified.get(key);
        }
    }

    private void switchToAlternateMap() {
        if (null != storeWhenModified) {
            return;
        }
        storeWhenModified = new HashMap<>(keysVsOffsets.size());
        for (Entry<String, Long> entry : keysVsOffsets.entrySet()) {
            storeWhenModified.put(entry.getKey(), getFromBackingBuffer(entry.getValue()));
        }
    }

    private ByteBuffer getFromBackingBuffer(Long valueMask) {
        int offset = this.offset + decodeOffsetFromMask(valueMask);
        int length = decodeLengthFromMask(valueMask);

        ByteBuffer bb = backingBuffer.byteBuffer();
        if (null == bb) {
            bb = ByteBuffer.wrap(backingBuffer.byteArray());
        }
        return ByteBufferUtil.preservingSlice(bb, offset, offset + length);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CBORMap)) {
            return false;
        }

        CBORMap that = (CBORMap) o;

        if (offset != that.offset) {
            return false;
        }
        if (backingBuffer != null? !backingBuffer.equals(that.backingBuffer) : that.backingBuffer != null) {
            return false;
        }
        if (keysVsOffsets != null? !keysVsOffsets.equals(that.keysVsOffsets) : that.keysVsOffsets != null) {
            return false;
        }
        if (storeWhenModified != null? !storeWhenModified.equals(that.storeWhenModified) :
                that.storeWhenModified != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = backingBuffer != null? backingBuffer.hashCode() : 0;
        result = 31 * result + offset;
        result = 31 * result + (keysVsOffsets != null? keysVsOffsets.hashCode() : 0);
        result = 31 * result + (storeWhenModified != null? storeWhenModified.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        String sb = "CBORMap{" + "backingBuffer=" + backingBuffer +
                    ", offset=" + offset +
                    ", keysVsOffsets=" + keysVsOffsets +
                    ", storeWhenModified=" + (null == storeWhenModified ? "null" : storeWhenModified) +
                    '}';
        return sb;
    }

    static long encodeValueMask(int offset, long length) {
        return length << 32 | offset & 0xFFFFFFFFL;
    }

    static int decodeLengthFromMask(long valueMask) {
        return (int) (valueMask >> 32);
    }

    static int decodeOffsetFromMask(long valueMask) {
        return (int) valueMask;
    }
}
