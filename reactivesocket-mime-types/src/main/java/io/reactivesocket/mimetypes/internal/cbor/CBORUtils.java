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

import java.util.function.Function;

import static io.reactivesocket.mimetypes.internal.cbor.CborMajorType.*;

public final class CBORUtils {

    public static final StackTraceElement[] EMPTY_STACK = new StackTraceElement[0];
    public static final Function<Byte, Boolean> BREAK_SCANNER = aByte -> aByte != CBOR_BREAK;

    @SuppressWarnings("ThrowableInstanceNeverThrown")
    private static final MalformedInputException TOO_LONG_LENGTH =
            new MalformedInputException("Length of a field is longer than: " + Integer.MAX_VALUE + " bytes.");

    static {
        TOO_LONG_LENGTH.setStackTrace(EMPTY_STACK);
    }

    private CBORUtils() {
    }

    /**
     * Parses the passed {@code buffer} and returns the length of the data following the index at the
     * {@link IndexedUnsafeBuffer#getReaderIndex()}.  <p>
     *
     * <h2>Special cases</h2>
     * <ul>
       <li>If the next data is a <a href="https://tools.ietf.org/html/rfc7049#section-2.3">"Break"</a> then -1 is
     returned.</li>
     </ul>
     *
     * @param buffer Buffer which will be parsed to determine the length of the next data item.
     * @param expectedType {@link CborMajorType} to expect.
     * @param errorIfMismatchType Error to throw if the type is not as expected.
     *
     * @return Length of the following data item. {@code -1} if the type is a Break.
     */
    public static long parseDataLengthOrDie(IndexedUnsafeBuffer buffer, CborMajorType expectedType,
                                            RuntimeException errorIfMismatchType) {
        final byte header = (byte) buffer.readUnsignedByte();
        final CborMajorType type = fromUnsignedByte(header);

        if (type == Break) {
            return -1;
        }

        if (type != expectedType) {
            throw errorIfMismatchType;
        }

        return CborHeader.readDataLength(buffer, header);
    }

    /**
     * Returns the length in bytes that the passed {@code bytesToEncode} will be when encoded as CBOR.
     *
     * @param bytesToEncode Length in bytes to encode.
     *
     * @return Length in bytes post encode.
     */
    public static long getEncodeLength(long bytesToEncode) {
        CborHeader header = CborHeader.forLengthToEncode(bytesToEncode);
        return bytesToEncode + header.getSizeInBytes();
    }

    /**
     * Encodes the passed {@code type} with {@code length} as a CBOR data header. The encoding is written on to the
     * passed {@code buffer}
     *
     * @param buffer Buffer to encode to.
     * @param type Type to encode.
     * @param length Length of data that will be encoded.
     *
     * @return Number of bytes written on to the buffer for this encoding.
     */
    public static int encodeTypeHeader(IndexedUnsafeBuffer buffer, CborMajorType type, long length) {
        CborHeader header = CborHeader.forLengthToEncode(length);
        header.encode(buffer, type, length);
        return header.getSizeInBytes();
    }

    /**
     * Encodes the passed {@code type} with indefinite length. The encoding is written on to the passed {@code buffer}
     *
     * @param buffer Buffer to encode to.
     * @param type Type to encode.
     *
     * @return Number of bytes written on to the buffer for this encoding.
     */
    public static int encodeIndefiniteTypeHeader(IndexedUnsafeBuffer buffer, CborMajorType type) {
        return encodeTypeHeader(buffer, type, -1);
    }

    /**
     * Returns the length(in bytes) till the next CBOR break i.e. {@link CborMajorType#CBOR_BREAK}.
     * This method does not move the {@code readerIndex} for the passed buffer.
     * @param src Buffer to scan.
     *
     * @return Index of the next CBOR break in the source buffer. {@code -1} if break is not found.
     */
    public static int scanToBreak(IndexedUnsafeBuffer src) {
        int i = src.forEachByte(BREAK_SCANNER);
        return i == src.getBackingBuffer().capacity() ? -1 : i;
    }
}
