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

import java.nio.charset.StandardCharsets;

import static io.reactivesocket.mimetypes.internal.cbor.CBORUtils.*;
import static io.reactivesocket.mimetypes.internal.cbor.CborMajorType.*;

final class CborUtf8StringCodec {

    @SuppressWarnings("ThrowableInstanceNeverThrown")
    static final MalformedInputException NOT_UTF8_STRING =
            new MalformedInputException("Data is not a definite length UTF-8 encoded string.");

    @SuppressWarnings("ThrowableInstanceNeverThrown")
    static final MalformedInputException BREAK_NOT_FOUND_FOR_INDEFINITE_LENGTH =
            new MalformedInputException("End of string not found for indefinite length string.");

    static {
        NOT_UTF8_STRING.setStackTrace(EMPTY_STACK);
        BREAK_NOT_FOUND_FOR_INDEFINITE_LENGTH.setStackTrace(EMPTY_STACK);
    }

    private CborUtf8StringCodec() {
    }

    public static void encode(IndexedUnsafeBuffer dst, String utf8String) {
        byte[] bytes = utf8String.getBytes(StandardCharsets.UTF_8);
        encodeTypeHeader(dst, Utf8String, bytes.length);
        dst.writeBytes(bytes, 0, bytes.length);
    }

    public static String decode(IndexedUnsafeBuffer src) {
        return decode(src, false);
    }

    public static String decode(IndexedUnsafeBuffer src, boolean returnNullIfBreak) {
        int length = (int) parseDataLengthOrDie(src, Utf8String, NOT_UTF8_STRING);
        if (length < 0) {
            if (returnNullIfBreak) {
                return null;
            } else {
                throw NOT_UTF8_STRING;
            }
        }

        if (length == CborHeader.INDEFINITE.getCode()) {
            String chunk = null;
            while (true) {
                byte aByte = src.getBackingBuffer().getByte(src.getReaderIndex());
                if (aByte == CBOR_BREAK) {
                    break;
                }

                int chunkLength = (int) parseDataLengthOrDie(src, Utf8String, NOT_UTF8_STRING);
                String thisChunk = readIntoString(src, chunkLength);
                chunk = null == chunk ? thisChunk : chunk + thisChunk;
            }

            return chunk;
        } else {
            return readIntoString(src, length);
        }
    }

    private static String readIntoString(IndexedUnsafeBuffer src, int chunkLength) {
        byte[] keyBytes = new byte[chunkLength];
        src.readBytes(keyBytes, 0, keyBytes.length);
        return new String(keyBytes, StandardCharsets.UTF_8);
    }
}
