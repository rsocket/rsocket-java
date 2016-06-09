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
import io.reactivesocket.mimetypes.internal.Codec;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map.Entry;

import static io.reactivesocket.mimetypes.internal.cbor.CBORUtils.*;

/**
 * This is a custom codec for default {@link KVMetadata} as defined by
 * <a href="https://github.com/ReactiveSocket/reactivesocket/blob/master/MimeTypes.md">the spec</a>. Since, the format
 * is simple, it does not use a third-party library to do the encoding, but the logic is contained in this class.
 */
public class MetadataCodec implements Codec {

    public static final MetadataCodec INSTANCE = new MetadataCodec();

    private static final ThreadLocal<IndexedUnsafeBuffer> indexedBuffers =
            ThreadLocal.withInitial(() -> new IndexedUnsafeBuffer(ByteOrder.BIG_ENDIAN));

    protected MetadataCodec() {
    }

    @Override
    public <T> T decode(ByteBuffer buffer, Class<T> tClass) {
        isValidType(tClass);

        IndexedUnsafeBuffer tmp = indexedBuffers.get();
        tmp.wrap(buffer);

        return _decode(tmp);
    }

    @Override
    public <T> T decode(DirectBuffer buffer, int offset, Class<T> tClass) {
        isValidType(tClass);

        IndexedUnsafeBuffer tmp = indexedBuffers.get();
        tmp.wrap(buffer);

        return _decode(tmp);
    }

    @Override
    public <T> ByteBuffer encode(T toEncode) {
        isValidType(toEncode.getClass());

        if (toEncode instanceof SlicedBufferKVMetadata) {
            return ((SlicedBufferKVMetadata) toEncode).getBackingBuffer().byteBuffer();
        }

        ByteBuffer dst = ByteBuffer.allocate(getSizeAsBytes((KVMetadata) toEncode));
        encodeTo(dst, toEncode);
        return dst;
    }

    @Override
    public <T> DirectBuffer encodeDirect(T toEncode) {
        isValidType(toEncode.getClass());

        final KVMetadata input = (KVMetadata) toEncode;

        if (toEncode instanceof SlicedBufferKVMetadata) {
            return ((SlicedBufferKVMetadata) toEncode).getBackingBuffer();
        }

        MutableDirectBuffer toReturn = new UnsafeBuffer(ByteBuffer.allocate(getSizeAsBytes(input)));
        encodeTo(toReturn, toEncode, 0);
        return toReturn;
    }

    @Override
    public <T> void encodeTo(ByteBuffer buffer, T toEncode) {
        isValidType(toEncode.getClass());

        final KVMetadata input = (KVMetadata) toEncode;

        IndexedUnsafeBuffer tmp = indexedBuffers.get();
        tmp.wrap(buffer);

        _encodeTo(tmp, input);
    }

    @Override
    public <T> void encodeTo(MutableDirectBuffer buffer, T toEncode, int offset) {
        isValidType(toEncode.getClass());

        final KVMetadata input = (KVMetadata) toEncode;

        IndexedUnsafeBuffer tmp = indexedBuffers.get();
        tmp.wrap(buffer, offset, buffer.capacity());

        _encodeTo(tmp, input);
    }

    private static void _encodeTo(IndexedUnsafeBuffer buffer, KVMetadata toEncode) {
        if (toEncode instanceof SlicedBufferKVMetadata) {
            SlicedBufferKVMetadata s = (SlicedBufferKVMetadata) toEncode;
            DirectBuffer backingBuffer = s.getBackingBuffer();
            backingBuffer.getBytes(0, buffer.getBackingBuffer(), buffer.getWriterIndex(), backingBuffer.capacity());
            return;
        }

        CborMapCodec.encode(buffer, toEncode);
    }

    private static <T> T _decode(IndexedUnsafeBuffer src) {
        CBORMap m = CborMapCodec.decode(src,
                                             (backingBuffer, offset, initialCapacity) -> new SlicedBufferKVMetadata(
                                                     backingBuffer, offset, initialCapacity));
        @SuppressWarnings("unchecked")
        T t = (T) m;
        return t;
    }

    private static int getSizeAsBytes(KVMetadata toEncode) {
        int toReturn = 1 + (int) getEncodeLength(toEncode.size()); // Map Starting + break
        for (Entry<String, ByteBuffer> entry : toEncode.entrySet()) {
            toReturn += getEncodeLength(entry.getKey().length());
            toReturn += entry.getKey().length();

            int valueLength = entry.getValue().remaining();
            toReturn += getEncodeLength(valueLength);
            toReturn += valueLength;
        }
        return toReturn;
    }

    private static <T> void isValidType(Class<T> tClass) {
        if (!KVMetadata.class.isAssignableFrom(tClass)) {
            throw new IllegalArgumentException("Metadata codec only supports encoding/decoding of: "
                                               + KVMetadata.class.getName());
        }
    }
}
