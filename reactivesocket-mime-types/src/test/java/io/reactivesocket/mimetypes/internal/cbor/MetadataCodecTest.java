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

import io.reactivesocket.mimetypes.KVMetadata;
import io.reactivesocket.mimetypes.internal.KVMetadataImpl;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static io.reactivesocket.mimetypes.internal.cbor.ByteBufferMapMatcher.*;
import static org.hamcrest.MatcherAssert.*;

public class MetadataCodecTest {

    @Rule
    public final CodecRule metaCodecRule = new CodecRule();
    @Rule
    public final io.reactivesocket.mimetypes.internal.CodecRule<CborCodec> cborCodecRule =
            new io.reactivesocket.mimetypes.internal.CodecRule<>(CborCodec::create);

    @Test(timeout = 60000)
    public void testDecode() throws Exception {
        metaCodecRule.addTestData("Key1", "Value1");
        metaCodecRule.addTestData("Key2", "Value2");
        metaCodecRule.addTestData("Key3", "Value3");

        ByteBuffer encode = cborCodecRule.getCodec().encode(metaCodecRule.testDataHolder);

        KVMetadata decode = metaCodecRule.codec.decode(encode, KVMetadata.class);

        assertThat("Unexpected decode.", decode, mapEqualTo(metaCodecRule.testDataHolder));
    }

    @Test(timeout = 60000)
    public void testDecodeDirect() throws Exception {
        metaCodecRule.addTestData("Key1", "Value1");
        metaCodecRule.addTestData("Key2", "Value2");
        metaCodecRule.addTestData("Key3", "Value3");

        ByteBuffer encode = cborCodecRule.getCodec().encode(metaCodecRule.testDataHolder);
        UnsafeBuffer ub = new UnsafeBuffer(encode);
        KVMetadata decode = metaCodecRule.codec.decode(ub, 0, KVMetadata.class);

        assertThat("Unexpected decode.", decode, mapEqualTo(metaCodecRule.testDataHolder));
    }

    @Test(timeout = 60000)
    public void encode() throws Exception {
        metaCodecRule.addTestData("Key1", "Value1");
        metaCodecRule.addTestData("Key2", "Value2");
        metaCodecRule.addTestData("Key3", "Value3");

        ByteBuffer encode = metaCodecRule.codec.encode(metaCodecRule.testData);
        KVMetadata decode = cborCodecRule.getCodec().decode(encode, KVMetadataImpl.class);

        assertThat("Unexpected decode.", decode, mapEqualTo(metaCodecRule.testData));
    }

    @Test(timeout = 60000)
    public void encodeDirect() throws Exception {
        metaCodecRule.addTestData("Key1", "Value1");
        metaCodecRule.addTestData("Key2", "Value2");
        metaCodecRule.addTestData("Key3", "Value3");

        DirectBuffer encode = metaCodecRule.codec.encodeDirect(metaCodecRule.testData);
        KVMetadata decode = cborCodecRule.getCodec().decode(encode.byteBuffer(), KVMetadataImpl.class);

        assertThat("Unexpected decode.", decode, mapEqualTo(metaCodecRule.testData));
    }

    @Test(timeout = 60000)
    public void encodeTo() throws Exception {
        metaCodecRule.addTestData("Key1", "Value1");
        metaCodecRule.addTestData("Key2", "Value2");
        metaCodecRule.addTestData("Key3", "Value3");

        ByteBuffer dst = ByteBuffer.allocate(500);
        metaCodecRule.codec.encodeTo(dst, metaCodecRule.testData);
        KVMetadata decode = cborCodecRule.getCodec().decode(dst, KVMetadataImpl.class);

        assertThat("Unexpected decode.", decode, mapEqualTo(metaCodecRule.testData));
    }

    @Test(timeout = 60000)
    public void encodeToDirect() throws Exception {
        metaCodecRule.addTestData("Key1", "Value1");
        metaCodecRule.addTestData("Key2", "Value2");
        metaCodecRule.addTestData("Key3", "Value3");

        ByteBuffer dst = ByteBuffer.allocate(500);
        UnsafeBuffer ub = new UnsafeBuffer(dst);
        metaCodecRule.codec.encodeTo(ub, metaCodecRule.testData, 0);
        KVMetadata decode = cborCodecRule.getCodec().decode(dst, KVMetadataImpl.class);

        assertThat("Unexpected decode.", decode, mapEqualTo(metaCodecRule.testData));
    }

    public static class CodecRule extends ExternalResource {

        private MetadataCodec codec;
        private Map<String, ByteBuffer> testDataHolder;
        private KVMetadata testData;

        @Override
        public Statement apply(final Statement base, Description description) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    codec = MetadataCodec.INSTANCE;
                    testDataHolder = new HashMap<>();
                    testData = new KVMetadataImpl(testDataHolder);
                    base.evaluate();
                }
            };
        }

        public void addTestData(String key, String value) {
            ByteBuffer vBuff = ByteBuffer.allocate(value.length()).put(value.getBytes());
            vBuff.flip();
            testDataHolder.put(key, vBuff);
        }
    }

}