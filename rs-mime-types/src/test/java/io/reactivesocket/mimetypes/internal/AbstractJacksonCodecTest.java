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

package io.reactivesocket.mimetypes.internal;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;

import java.nio.ByteBuffer;

public abstract class AbstractJacksonCodecTest<T extends Codec> {

    @Rule
    public final CustomObjectRule customObjectRule = new CustomObjectRule();
    @Rule
    public final MetadataRule metadataRule = new MetadataRule();

    @Test(timeout = 60000)
    public void encodeDecode() throws Exception {
        customObjectRule.populateDefaultData();
        ByteBuffer encode = getCodecRule().getCodec().encode(customObjectRule.getData());

        CustomObject decode = getCodecRule().getCodec().decode(encode, CustomObject.class);
        MatcherAssert.assertThat("Unexpected decode.", decode, Matchers.equalTo(customObjectRule.getData()));
    }

    @Test(timeout = 60000)
    public void encodeDecodeDirect() throws Exception {
        customObjectRule.populateDefaultData();
        DirectBuffer encode = getCodecRule().getCodec().encodeDirect(customObjectRule.getData());

        CustomObject decode = getCodecRule().getCodec().decode(encode, 0, CustomObject.class);
        MatcherAssert.assertThat("Unexpected decode.", decode, Matchers.equalTo(customObjectRule.getData()));
    }

    @Test(timeout = 60000)
    public void encodeTo() throws Exception {
        customObjectRule.populateDefaultData();
        ByteBuffer encodeDest = ByteBuffer.allocate(10000);
        getCodecRule().getCodec().encodeTo(encodeDest, customObjectRule.getData());

        encodeDest.flip(); /*Since we want to decode it now*/

        CustomObject decode = getCodecRule().getCodec().decode(encodeDest, CustomObject.class);

        MatcherAssert.assertThat("Unexpected decode.", decode, Matchers.equalTo(customObjectRule.getData()));
    }

    @Test(timeout = 60000)
    public void encodeToDirect() throws Exception {
        customObjectRule.populateDefaultData();
        byte[] destArr = new byte[1000];
        MutableDirectBuffer encodeDest = new UnsafeBuffer(destArr);
        getCodecRule().getCodec().encodeTo(encodeDest, customObjectRule.getData(), 0);

        CustomObject decode = getCodecRule().getCodec().decode(encodeDest, 0, CustomObject.class);

        MatcherAssert.assertThat("Unexpected decode.", decode, Matchers.equalTo(customObjectRule.getData()));
    }

    protected abstract CodecRule<T> getCodecRule();
}
