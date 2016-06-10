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
public class CborHeaderTest {

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
    public void testEncodeDecode() throws Exception {
        CborHeader cborHeader = CborHeader.forLengthToEncode(length);
        CborHeader expected = null;
        if (length == -1) {
            expected = CborHeader.INDEFINITE;
        } else if (length < 24) {
            expected = CborHeader.SMALL;
        } else if (length <= Byte.MAX_VALUE) {
            expected = CborHeader.BYTE;
        } else if (length <= Short.MAX_VALUE) {
            expected = CborHeader.SHORT;
        } else if (length <= Integer.MAX_VALUE) {
            expected = CborHeader.INT;
        } else if (length <= Long.MAX_VALUE) {
            expected = CborHeader.LONG;
        }

        MatcherAssert.assertThat("Unexpected CBOR header type for length: " + length, cborHeader, is(expected));

        if (length < 0) {
            return;
        }

        ByteBuffer allocate = ByteBuffer.allocate(CborHeader.LONG.getSizeInBytes());
        IndexedUnsafeBuffer iub = new IndexedUnsafeBuffer(ByteOrder.BIG_ENDIAN);
        iub.wrap(allocate);

        cborHeader.encode(iub, type, length);

        MatcherAssert.assertThat("Unxexpected bytes written for type: " + type + " and length: " + length,
                                 (short) iub.getWriterIndex(), equalTo(cborHeader.getSizeInBytes()));

        iub.setReaderIndex(0);
        long l = CborHeader.readDataLength(iub, iub.readUnsignedByte());
        MatcherAssert.assertThat("Unexpected data length read from encode.", l, equalTo(length));
    }
}