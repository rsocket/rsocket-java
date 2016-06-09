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
public class CborUtf8StringCodecTest {

    @Parameters
    public static Collection<Integer[]> data() {
        return Arrays.asList(new Integer[][] {
                { 0 },
                { Integer.valueOf(Byte.MAX_VALUE) },
                { Integer.valueOf(Short.MAX_VALUE) },
        });
    }

    @Parameter
    public int stringLength;

    @Rule
    public final CodecRule<CborCodec> cborCodecRule = new CodecRule<>(CborCodec::create);

    @Test(timeout = 60000)
    public void testEncodeWithJacksonAndDecode() throws Exception {
        String toEncode = newString(stringLength);
        ByteBuffer encode = cborCodecRule.getCodec().encode(toEncode);
        IndexedUnsafeBuffer iub = new IndexedUnsafeBuffer(ByteOrder.BIG_ENDIAN);
        iub.wrap(encode);

        String decode = CborUtf8StringCodec.decode(iub);

        MatcherAssert.assertThat("Unexpected decode.", decode, equalTo(toEncode));
    }

    @Test(timeout = 60000)
    public void testEncodeWithDecodeWithJackson() throws Exception {
        String toEncode = newString(stringLength);
        ByteBuffer dst = ByteBuffer.allocate(stringLength + 10);
        IndexedUnsafeBuffer iub = new IndexedUnsafeBuffer(ByteOrder.BIG_ENDIAN);
        iub.wrap(dst);
        CborUtf8StringCodec.encode(iub, toEncode);
        dst.rewind();

        String decode = cborCodecRule.getCodec().decode(dst, String.class);

        MatcherAssert.assertThat("Unexpected decode.", decode, equalTo(toEncode));
    }

    private static String newString(int stringLength) {
        byte[] b = new byte[stringLength];
        Arrays.fill(b, (byte) 'a');
        return new String(b);
    }
}