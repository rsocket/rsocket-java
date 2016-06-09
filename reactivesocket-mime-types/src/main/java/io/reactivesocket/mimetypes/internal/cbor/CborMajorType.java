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

import java.util.HashMap;
import java.util.Map;

/**
 * A representation of all supported CBOR major types as defined in the <a href="https://tools.ietf.org/html/rfc7049#section-2">spec</a>.
 */
public enum CborMajorType {

    UnsignedInteger(0),
    NegativeInteger(1),
    ByteString(2),
    Utf8String(3),
    ARRAY(4),
    MAP(5),
    Break(7),
    Unknown(-1);

    private final int typeCode;
    private final static Map<Integer, CborMajorType> reverseIndex;

    public static final byte CBOR_BREAK = (byte) 0b111_11111;

    static {
        reverseIndex = new HashMap<>(CborMajorType.values().length);
        for (CborMajorType type : CborMajorType.values()) {
            reverseIndex.put(type.typeCode, type);
        }
    }

    CborMajorType(int typeCode) {
        this.typeCode = typeCode;
    }

    public int getTypeCode() {
        return typeCode;
    }

    /**
     * Reads the first byte of the CBOR type header ({@link CborHeader}) to determine which type is encoded in the
     * header.
     *
     * @param unsignedByte First byte of the type header.
     *
     * @return The major type as encoded in the header.
     */
    public static CborMajorType fromUnsignedByte(short unsignedByte) {
        int type = unsignedByte >> 5 & 0x7;
        CborMajorType t = reverseIndex.get(type);
        if (null == t) {
            return Unknown;
        }

        if (t == Break) {
            final int length = CborHeader.readLengthFromFirstHeaderByte(unsignedByte);
            if (31 == length) {
                return Break;
            }
            return Unknown;
        }

        return t;
    }
}
