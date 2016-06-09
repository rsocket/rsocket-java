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

import io.reactivesocket.mimetypes.internal.CodecRule;
import org.hamcrest.MatcherAssert;
import org.junit.Rule;
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
public class CborBinaryStringCodecTest {

    @Parameters
    public static Collection<Integer[]> data() {
        return Arrays.asList(new Integer[][] {
                { 0 },
                { Integer.valueOf(Byte.MAX_VALUE) },
                { Integer.valueOf(Short.MAX_VALUE) },
        });
    }

    @Parameter
    public int bufLength;

    @Rule
    public final CodecRule<CborCodec> cborCodecRule = new CodecRule<>(CborCodec::create);

    @Test(timeout = 60000)
    public void testInfiniteDecode() throws Exception {
        ByteBuffer toEncode = newBuffer(bufLength);
        ByteBuffer encode = encodeChunked(toEncode);

        testDecode(toEncode, encode);
    }

    @Test(timeout = 60000)
    public void testEncodeWithJacksonAndDecode() throws Exception {
        ByteBuffer toEncode = newBuffer(bufLength);
        ByteBuffer encode = cborCodecRule.getCodec().encode(toEncode);
        testDecode(toEncode, encode);
    }

    @Test(timeout = 60000)
    public void testEncodeAndDecodeWithJackson() throws Exception {
        ByteBuffer toEncode = newBuffer(bufLength);
        ByteBuffer dst = ByteBuffer.allocate(bufLength + 10);
        IndexedUnsafeBuffer iub = new IndexedUnsafeBuffer(ByteOrder.BIG_ENDIAN);
        iub.wrap(dst);
        CborBinaryStringCodec.encode(iub, toEncode);
        dst.rewind();

        ByteBuffer decode = cborCodecRule.getCodec().decode(dst, ByteBuffer.class);

        toEncode.rewind();
        MatcherAssert.assertThat("Unexpected decode.", decode, equalTo(toEncode));
    }

    private static ByteBuffer newBuffer(int len) {
        byte[] b = new byte[len];
        Arrays.fill(b, (byte) 'a');
        return ByteBuffer.wrap(b);
    }

    private static void testDecode(ByteBuffer toEncode, ByteBuffer encode) {
        IndexedUnsafeBuffer iub = new IndexedUnsafeBuffer(ByteOrder.BIG_ENDIAN);
        iub.wrap(encode);

        ByteBuffer dst = ByteBuffer.allocate(toEncode.remaining());
        IndexedUnsafeBuffer idst = new IndexedUnsafeBuffer(ByteOrder.BIG_ENDIAN);
        idst.wrap(dst);
        CborBinaryStringCodec.decode(iub, idst);

        MatcherAssert.assertThat("Unexpected decode.", dst, equalTo(toEncode));
    }

    private ByteBuffer encodeChunked(ByteBuffer toEncode) {
        int chunkCount = 5;
        int chunkSize = bufLength / chunkCount;
        CborHeader chunkHeader = CborHeader.forLengthToEncode(chunkSize);
        IndexedUnsafeBuffer iub = new IndexedUnsafeBuffer(ByteOrder.BIG_ENDIAN);
        iub.wrap(toEncode);

        ByteBuffer encode = ByteBuffer.allocate(bufLength + 100);
        IndexedUnsafeBuffer encodeB = new IndexedUnsafeBuffer(ByteOrder.BIG_ENDIAN);
        encodeB.wrap(encode);

        int offset = 0;
        CborHeader.INDEFINITE.encodeIndefiniteLength(encodeB, CborMajorType.ByteString);

        int remaining = bufLength - offset;

        while (remaining > 0) {
            int thisChunkSize = Math.min(chunkSize, remaining);
            chunkHeader.encode(encodeB, CborMajorType.ByteString, thisChunkSize);
            iub.readBytes(encodeB, thisChunkSize);
            encodeB.incrementWriterIndex(thisChunkSize);
            offset += thisChunkSize;
            remaining = bufLength - offset;
        }

        CborHeader.SMALL.encode(encodeB, CborMajorType.Break, 31);

        return encode;
    }
}