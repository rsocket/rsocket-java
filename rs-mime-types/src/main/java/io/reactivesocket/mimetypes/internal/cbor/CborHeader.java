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
import rx.functions.Action2;
import rx.functions.Actions;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * CBOR uses a compact format to encode the length and type of data that is written. More details can be found in the
 * <a href="https://tools.ietf.org/html/rfc7049#section-2">spec</a> but it follows the following format:
 *
 * <h3>First Byte</h3>
 * The first byte of the header has the following data encoded:
 * <ul>
 <li>Data type as specified by {@link CborMajorType#getTypeCode()}.</li>
 <li>Actual length of the following data element.</li>
 </ul>
 *
 * The byte layout is as follows:
 * <PRE>
 0
 0 1 2 3 4 5 6 7
 +-+-+-+-+-+-+-+
 | T |  Code   |
 +-------------+
 * </PRE>
 *
 * &lt;T&gt; above is the Data type. <p>
 * &lt;Code&gt; above is the actual length for Header {@link #SMALL} and the code {@link #getCode()} for all other
 * headers.
 *
 * <h3>Remaining Bytes</h3>
 *
 * Headers {@link #SMALL} and {@link #INDEFINITE} does not contain any other bytes after the first bytes. The other
 * headers contain {@link #getSizeInBytes()} {@code - 1} more bytes containing the actual length of the following data.
 *
 * This class abstracts all the above rules to correctly encode and decode this type headers.
 */
public enum CborHeader {

    INDEFINITE(1, 31, Actions.empty(),
               aLong -> aLong < 0,
               buffer -> 31L,
               aLong -> (byte) 31),
    SMALL(1, -1,
          Actions.empty(),
          aLong -> aLong < 24, buffer -> -1L,
          aLong -> aLong.byteValue()),
    BYTE(1 + BitUtil.SIZE_OF_BYTE, 24,
         (buffer, aLong) -> buffer.writeByte((byte) aLong.shortValue()),
         aLong -> aLong <= Byte.MAX_VALUE,
         buffer -> (long)buffer.readByte(), aLong -> (byte) 24),
    SHORT(1 + BitUtil.SIZE_OF_SHORT, 25,
          (buffer, aLong) -> buffer.writeShort(aLong.shortValue()),
          aLong -> aLong <= Short.MAX_VALUE,
          buffer -> (long)buffer.readShort(),
          aLong -> (byte) 25),
    INT(1 + BitUtil.SIZE_OF_INT, 26,
        (buffer, aLong) -> buffer.writeInt(aLong.intValue()),
        aLong -> aLong <= Integer.MAX_VALUE,
        buffer -> (long)buffer.readInt(),
        aLong -> (byte) 26),
    LONG(1 + BitUtil.SIZE_OF_LONG, 27,
         (buffer, aLong) -> buffer.writeLong(aLong),
         aLong -> aLong <= Long.MAX_VALUE,
         buffer -> (long)buffer.readLong(),
         aLong -> (byte) 27);

    private static final int LENGTH_MASK = 0b000_11111;
    private final static Map<Integer, CborHeader> reverseIndex;

    static {
        reverseIndex = new HashMap<>(CborHeader.values().length);
        for (CborHeader h : CborHeader.values()) {
            reverseIndex.put(h.code, h);
        }
    }

    private final short sizeInBytes;
    private final int code;
    private final Action2<IndexedUnsafeBuffer, Long> encodeFunction;
    private final Function<Long, Boolean> matchFunction;
    private final Function<IndexedUnsafeBuffer, Long> decodeFunction;
    private final Function<Long, Byte> codeFunction;

    CborHeader(int sizeInBytes, int code, Action2<IndexedUnsafeBuffer, Long> encodeFunction,
               Function<Long, Boolean> matchFunction, Function<IndexedUnsafeBuffer, Long> decodeFunction,
               Function<Long, Byte> codeFunction) {
        this.sizeInBytes = (short) sizeInBytes;
        this.code = code;
        this.encodeFunction = encodeFunction;
        this.matchFunction = matchFunction;
        this.decodeFunction = decodeFunction;
        this.codeFunction = codeFunction;
    }



    /**
     * Returns {@link CborHeader} instance appropriate for encoding the passed {@code bytesToEncode}.
     *
     * @param bytesToEncode Number of bytes to encode.
     *
     * @return {@link CborHeader} appropriate for encoding the passed number of bytes.
     */
    public static CborHeader forLengthToEncode(long bytesToEncode) {
        if (INDEFINITE.matchFunction.apply(bytesToEncode)) {
            return INDEFINITE;
        }

        if (SMALL.matchFunction.apply(bytesToEncode)) {
            return SMALL;
        }

        if (BYTE.matchFunction.apply(bytesToEncode)) {
            return BYTE;
        }

        if (SHORT.matchFunction.apply(bytesToEncode)) {
            return SHORT;
        }

        if (INT.matchFunction.apply(bytesToEncode)) {
            return INT;
        }

        return LONG;
    }

    /**
     * Returns the number of bytes that this header will encode to.
     *
     * @return The number of bytes that this header will encode to.
     */
    public short getSizeInBytes() {
        return sizeInBytes;
    }

    /**
     * The CBOR code that will be encoded in the first byte of the header.
     * <em>{@link CborHeader#SMALL} will return -1 as there is no code for it. Instead it encodes the actual length.</em>
     *
     * @return The number of bytes that this header will encode to.
     */
    public int getCode() {
        return code;
    }

    /**
     * Encodes the passed {@code type} and {@code length} into the passed {@code buffer}.
     *
     * @param buffer Destination for the encoding.
     * @param type Type to encode.
     * @param length Length to encode. Can be {@code -1} for {@link #INDEFINITE}, otherwise has to be a positive
     * number.
     *
     * @throws IllegalArgumentException If the length is negative (for all headers except {@link #INDEFINITE}.
     */
    public void encode(IndexedUnsafeBuffer buffer, CborMajorType type, long length) {
        if (length == -1 && this != INDEFINITE && length < 0) {
            throw new IllegalArgumentException("Length must be positive.");
        }

        byte code = codeFunction.apply(length);
        int firstByte = type.getTypeCode() << 5 | code;

        buffer.writeByte((byte) firstByte);
        encodeFunction.call(buffer, length);
    }

    /**
     * Encodes the passed {@code type} for indefinite length into the passed {@code buffer}. Same as calling
     * {@link #encode(IndexedUnsafeBuffer, CborMajorType, long)} with {@code -1} as length.
     */
    public void encodeIndefiniteLength(IndexedUnsafeBuffer buffer, CborMajorType type) {
        encode(buffer, type, -1);
    }

    /**
     * Given the first byte of a CBOR data type and length header, returns the length of the data item that follows the
     * header. This will read {@link #getSizeInBytes()} number of bytes from the passed buffer corresponding to the
     * {@link CborHeader} encoded in the first byte.
     */
    public static long readDataLength(IndexedUnsafeBuffer buffer, short firstHeaderByte) {
        int firstLength = readLengthFromFirstHeaderByte(firstHeaderByte);
        CborHeader cborHeader = reverseIndex.get(firstLength);
        if (null != cborHeader) {
            return cborHeader.decodeFunction.apply(buffer);
        }

        return firstLength;
    }

    /**
     * Reads the length code from the first byte of CBOR header. This can be the actual length if the header is of type
     * {@link #SMALL} or {@link #getCode()} for all other types.
     */
    public static int readLengthFromFirstHeaderByte(short firstHeaderByte) {
        return firstHeaderByte & LENGTH_MASK;
    }
}
