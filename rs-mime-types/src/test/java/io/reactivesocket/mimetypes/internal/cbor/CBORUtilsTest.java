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
import org.hamcrest.MatcherAssert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.Matchers.*;

@RunWith(Parameterized.class)
public class CBORUtilsTest {

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {CborMajorType.UnsignedInteger, 22},
                {CborMajorType.UnsignedInteger, -1},
                {CborMajorType.UnsignedInteger, 0},
                {CborMajorType.UnsignedInteger, Byte.MAX_VALUE},
                {CborMajorType.UnsignedInteger, Short.MAX_VALUE},
                {CborMajorType.UnsignedInteger, Integer.MAX_VALUE},

                {CborMajorType.NegativeInteger, 2},
                {CborMajorType.NegativeInteger, -1},
                {CborMajorType.NegativeInteger, 0},
                {CborMajorType.NegativeInteger, Byte.MAX_VALUE},
                {CborMajorType.NegativeInteger, Short.MAX_VALUE},
                {CborMajorType.NegativeInteger, Integer.MAX_VALUE},

                {CborMajorType.Utf8String, 2},
                {CborMajorType.Utf8String, -1},
                {CborMajorType.Utf8String, 0},
                {CborMajorType.Utf8String, Byte.MAX_VALUE},
                {CborMajorType.Utf8String, Short.MAX_VALUE},
                {CborMajorType.Utf8String, Integer.MAX_VALUE},
                {CborMajorType.Utf8String, Long.MAX_VALUE},

                {CborMajorType.ByteString, 2}, 
                {CborMajorType.ByteString, -1},
                {CborMajorType.ByteString, 0},
                {CborMajorType.ByteString, Byte.MAX_VALUE},
                {CborMajorType.ByteString, Short.MAX_VALUE},
                {CborMajorType.ByteString, Integer.MAX_VALUE},
                {CborMajorType.ByteString, Long.MAX_VALUE},

                {CborMajorType.MAP, 2},
                {CborMajorType.MAP, -1},
                {CborMajorType.MAP, 0},
                {CborMajorType.MAP, Byte.MAX_VALUE},
                {CborMajorType.MAP, Short.MAX_VALUE},
                {CborMajorType.MAP, Integer.MAX_VALUE},
                {CborMajorType.MAP, Long.MAX_VALUE},

                {CborMajorType.ARRAY, 2}, 
                {CborMajorType.ARRAY, -1},
                {CborMajorType.ARRAY, 0},
                {CborMajorType.ARRAY, Byte.MAX_VALUE},
                {CborMajorType.ARRAY, Short.MAX_VALUE},
                {CborMajorType.ARRAY, Integer.MAX_VALUE},
                {CborMajorType.ARRAY, Long.MAX_VALUE},
        });
    }

    @Parameter
    public CborMajorType type;
    @Parameter(1)
    public long length;

    @Test(timeout = 60000)
    public void parseDataLengthOrDie() throws Exception {
        IndexedUnsafeBuffer ib = newBufferWithHeader();
        long length = CBORUtils.parseDataLengthOrDie(ib, type, new NullPointerException());

        MatcherAssert.assertThat("Unexpected length post decode.", length, is(normalizeLength(this.length)));
    }

    @Test(timeout = 60000, expected = RuntimeException.class)
    public void parseDataLengthOrDieWrongType() throws Exception {
        IndexedUnsafeBuffer ib = newBufferWithHeader();
        CBORUtils.parseDataLengthOrDie(ib, CborMajorType.Unknown, new NullPointerException());
    }

    @Test(timeout = 60000)
    public void getEncodeLength() throws Exception {
        long encodeLength = CBORUtils.getEncodeLength(length);
        long expectedLength = length;
        if (length < 24 || expectedLength == 31) {
            expectedLength++;
        } else if (length <= Byte.MAX_VALUE) {
            expectedLength++;
            expectedLength += BitUtil.SIZE_OF_BYTE;
        } else if (length <= Short.MAX_VALUE) {
            expectedLength++;
            expectedLength += BitUtil.SIZE_OF_SHORT;
        } else if (length <= Integer.MAX_VALUE) {
            expectedLength++;
            expectedLength += BitUtil.SIZE_OF_INT;
        } else if (length <= Long.MAX_VALUE) {
            expectedLength++;
            expectedLength += BitUtil.SIZE_OF_LONG;
        }

        MatcherAssert.assertThat("Unexpected encoded length.", encodeLength, is(expectedLength));
    }

    @Test(timeout = 60000)
    public void encodeTypeHeader() throws Exception {
        ByteBuffer allocate = ByteBuffer.allocate(100);
        IndexedUnsafeBuffer iub = new IndexedUnsafeBuffer(ByteOrder.BIG_ENDIAN);
        iub.wrap(allocate);
        int expected = CborHeader.forLengthToEncode(length).getSizeInBytes();
        int encodedLength = CBORUtils.encodeTypeHeader(iub, type, length);

        MatcherAssert.assertThat("Unexpected number of bytes written.", encodedLength, is(expected));
    }

    private IndexedUnsafeBuffer newBufferWithHeader() {
        ByteBuffer src = ByteBuffer.allocate(100);
        IndexedUnsafeBuffer ib = new IndexedUnsafeBuffer(ByteOrder.BIG_ENDIAN);
        ib.wrap(src);
        CborHeader cborHeader = CborHeader.forLengthToEncode(length);
        cborHeader.encode(ib, type, normalizeLength(length));
        return ib;
    }

    private long normalizeLength(long length) {
        return -1 == length ? CborHeader.INDEFINITE.getCode() : this.length;
    }
}