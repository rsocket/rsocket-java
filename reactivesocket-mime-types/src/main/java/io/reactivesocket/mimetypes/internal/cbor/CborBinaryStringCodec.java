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
import org.agrona.MutableDirectBuffer;

import java.nio.ByteBuffer;

import static io.reactivesocket.mimetypes.internal.cbor.CBORUtils.*;
import static io.reactivesocket.mimetypes.internal.cbor.CborMajorType.*;

final class CborBinaryStringCodec {

    @SuppressWarnings("ThrowableInstanceNeverThrown")
    static final MalformedInputException NOT_BINARY_STRING =
            new MalformedInputException("Data is not a definite length binary string.");

    @SuppressWarnings("ThrowableInstanceNeverThrown")
    static final MalformedInputException INDEFINITE_LENGTH_NOT_SUPPORTED =
            new MalformedInputException("Indefinite length binary string parsing not supported.");

    static {
        NOT_BINARY_STRING.setStackTrace(EMPTY_STACK);
        INDEFINITE_LENGTH_NOT_SUPPORTED.setStackTrace(EMPTY_STACK);
    }

    private CborBinaryStringCodec() {
    }

    public static void encode(IndexedUnsafeBuffer dst, DirectBuffer src, int offset, int length) {
        encodeTypeHeader(dst, ByteString, length);
        dst.writeBytes(src, offset, length);
    }

    public static void encode(IndexedUnsafeBuffer dst, ByteBuffer src) {
        encodeTypeHeader(dst, ByteString, src.remaining());
        dst.writeBytes(src, src.remaining());
    }

    public static void decode(IndexedUnsafeBuffer src, IndexedUnsafeBuffer dst) {
        int length = decode(src, dst.getBackingBuffer(), 0);
        dst.incrementWriterIndex(length);
    }

    public static int decode(IndexedUnsafeBuffer src, MutableDirectBuffer dst, int offset) {
        int length = (int) parseDataLengthOrDie(src, ByteString, NOT_BINARY_STRING);
        if (length < 0) {
            throw NOT_BINARY_STRING;
        }

        if (length == CborHeader.INDEFINITE.getCode()) {
            while (true) {
                byte aByte = src.getBackingBuffer().getByte(src.getReaderIndex());
                if (aByte == CBOR_BREAK) {
                    break;
                }

                int chunkLength = (int) parseDataLengthOrDie(src, ByteString, NOT_BINARY_STRING);
                src.readBytes(dst, offset, chunkLength);
                offset += chunkLength;
            }
        } else {
            src.readBytes(dst, offset, length);
        }

        return length;
    }
}
