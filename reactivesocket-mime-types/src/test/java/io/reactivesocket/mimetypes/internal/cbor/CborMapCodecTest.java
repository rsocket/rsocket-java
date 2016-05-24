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
import io.reactivesocket.mimetypes.internal.KVMetadataImpl;
import org.hamcrest.MatcherAssert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static io.reactivesocket.mimetypes.internal.cbor.ByteBufferMapMatcher.*;

@RunWith(Parameterized.class)
public class CborMapCodecTest {

    @Parameters
    public static Collection<Integer[]> data() {
        return Arrays.asList(new Integer[][] {
                { 0 },
                { 1 },
                { Integer.valueOf(Byte.MAX_VALUE) },
                { Integer.valueOf(Short.MAX_VALUE) },
        });
    }

    @Parameter
    public int mapSize;

    @Rule
    public final CborMapRule mapRule = new CborMapRule();

    @Rule
    public final CodecRule<CborCodec> cborCodecRule = new CodecRule<>(CborCodec::create);

    @Test(timeout = 60000)
    public void testEncodeWithJacksonAndDecode() throws Exception {
        Map<String, ByteBuffer> map = mapRule.newMap(mapSize);
        ByteBuffer encode = cborCodecRule.getCodec().encode(map);
        IndexedUnsafeBuffer iub = new IndexedUnsafeBuffer(ByteOrder.BIG_ENDIAN);
        iub.wrap(encode);

        CBORMap decode = CborMapCodec.decode(iub, (b, o, i) -> new CBORMap(b, o, i));

        MatcherAssert.assertThat("Unexpected decode.", decode, mapEqualTo(map));
    }

    @Test(timeout = 60000)
    public void testEncodeAndDecodeWithJackson() throws Exception {
        Map<String, ByteBuffer> map = mapRule.newMap(mapSize);
        ByteBuffer encode = ByteBuffer.allocate(mapSize == 0 ? 20 : mapSize * 100);
        IndexedUnsafeBuffer iencode = new IndexedUnsafeBuffer(ByteOrder.BIG_ENDIAN);
        iencode.wrap(encode);

        CborMapCodec.encode(iencode, map);

        @SuppressWarnings("unchecked")
        Map<String, ByteBuffer> decode = cborCodecRule.getCodec().decode(encode, KVMetadataImpl.class);

        MatcherAssert.assertThat("Unexpected decode.", decode, mapEqualTo(map));
    }

    @Test(timeout = 60000)
    public void testEncodeCborMapAsIs() throws Exception {
        mapRule.addMockEntries(mapSize);
        ByteBuffer encode = ByteBuffer.allocate(mapRule.map.getBackingBuffer().capacity() + 100);
        IndexedUnsafeBuffer iub = new IndexedUnsafeBuffer(ByteOrder.BIG_ENDIAN);
        iub.wrap(encode);

        CborMapCodec.encode(iub, mapRule.map);

        @SuppressWarnings("unchecked")
        Map<String, ByteBuffer> decode = cborCodecRule.getCodec().decode(encode, KVMetadataImpl.class);

        MatcherAssert.assertThat("Unexpected decode.", decode, mapEqualTo(mapRule.map));
    }

    public class CborMapRule extends AbstractCborMapRule<CBORMap> {

        @Override
        protected void init() {
            valueBuffer = ByteBuffer.allocate(mapSize == 0 ? 20 : mapSize * 100);
            indexed = new IndexedUnsafeBuffer(ByteOrder.BIG_ENDIAN);
            indexed.wrap(valueBuffer);
            map = new CBORMap(indexed.getBackingBuffer(), 0);
        }

        private Map<String, ByteBuffer> newMap(int mapSize) {
            Map<String, ByteBuffer> map = new HashMap<>(mapSize);
            for (int i = 0; i < mapSize; i++) {
                String key = "Key" + i;
                byte[] val = ("Value" + i).getBytes(StandardCharsets.UTF_8);
                ByteBuffer v = ByteBuffer.wrap(val);
                map.put(key, v);
            }
            return map;
        }
    }
}