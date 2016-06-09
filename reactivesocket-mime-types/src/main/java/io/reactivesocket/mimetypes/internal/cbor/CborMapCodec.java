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

import io.reactivesocket.mimetypes.internal.MalformedInputException;
import org.agrona.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;

import static io.reactivesocket.mimetypes.internal.cbor.CBORUtils.*;
import static io.reactivesocket.mimetypes.internal.cbor.CborMajorType.*;

final class CborMapCodec {

    @SuppressWarnings("ThrowableInstanceNeverThrown")
    private static final MalformedInputException NOT_MAP = new MalformedInputException("Data is not a Map.");

    @SuppressWarnings("ThrowableInstanceNeverThrown")
    private static final MalformedInputException VALUE_NOT_BINARY =
            new MalformedInputException("Value for a map entry is not binary.");

    static {
        NOT_MAP.setStackTrace(EMPTY_STACK);
        VALUE_NOT_BINARY.setStackTrace(EMPTY_STACK);
    }

    private CborMapCodec() {
    }

    public static void encode(IndexedUnsafeBuffer dst, CBORMap cborMap) {
        cborMap.encodeTo(dst);
    }

    public static void encode(IndexedUnsafeBuffer dst, Map<String, ByteBuffer> toEncode) {
        final int size = toEncode.size();
        CborHeader.forLengthToEncode(size).encode(dst, MAP, size);
        for (Entry<String, ByteBuffer> entry : toEncode.entrySet()) {
            CborUtf8StringCodec.encode(dst, entry.getKey());

            CborBinaryStringCodec.encode(dst, entry.getValue());
        }
    }

    public static CBORMap decode(IndexedUnsafeBuffer src, CborMapFactory mapFactory) {
        long mapSize = parseDataLengthOrDie(src, MAP, NOT_MAP);
        final CBORMap dst = mapFactory.newMap(src.getBackingBuffer(), src.getBackingBufferOffset(),
                                              mapSize == CborHeader.INDEFINITE.getCode() ? 16 : (int) mapSize);
        _decode(src, mapSize, dst);
        return dst;
    }

    public static void decode(IndexedUnsafeBuffer src, CBORMap dst) {
        long mapSize = parseDataLengthOrDie(src, MAP, NOT_MAP);
        _decode(src, mapSize, dst);
    }

    private static void _decode(IndexedUnsafeBuffer src, long mapSize, CBORMap dst) {
        boolean isIndefiniteMap = mapSize == CborHeader.INDEFINITE.getCode();
        int i = 0;
        while (true) {
            String key = CborUtf8StringCodec.decode(src, isIndefiniteMap);
            if (null == key) {
                break;
            }

            int valLength = (int) parseDataLengthOrDie(src, ByteString, VALUE_NOT_BINARY);
            int valueOffset = src.getReaderIndex();
            if (valLength < 0) {
                throw VALUE_NOT_BINARY;
            }

            if (valLength == CborHeader.INDEFINITE.getCode()) {
                throw CborBinaryStringCodec.INDEFINITE_LENGTH_NOT_SUPPORTED;
            }
            dst.putValueOffset(key, valueOffset, valLength);
            src.incrementReaderIndex(valLength);

            if (!isIndefiniteMap && ++i >= mapSize) {
                break;
            }
        }
    }

    public interface CborMapFactory {

        CBORMap newMap(DirectBuffer backingBuffer, int offset, int initialCapacity);

    }

}
