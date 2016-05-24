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

import io.reactivesocket.mimetypes.KVMetadata;
import io.reactivesocket.mimetypes.internal.cbor.ReactiveSocketDefaultMetadataCodec;
import org.agrona.DirectBuffer;
import org.hamcrest.MatcherAssert;
import org.junit.Rule;
import org.junit.Test;

import java.nio.ByteBuffer;

import static io.reactivesocket.mimetypes.internal.cbor.ByteBufferMapMatcher.*;

public class ReactiveSocketDefaultMetadataCodecTest {

    @Rule
    public final CodecRule<ReactiveSocketDefaultMetadataCodec> codecRule =
            new CodecRule<>(ReactiveSocketDefaultMetadataCodec::create);
    @Rule
    public final MetadataRule metadataRule = new MetadataRule();

    @Test(timeout = 60000)
    public void testDecodeDefault() throws Exception {

        metadataRule.populateDefaultMetadataData();

        ByteBuffer encode = codecRule.getCodec().encode(metadataRule.getKvMetadata());
        KVMetadata kvMetadata = codecRule.getCodec().decodeDefault(encode);

        MatcherAssert.assertThat("Unexpected decoded metadata.", kvMetadata, mapEqualTo(metadataRule.getKvMetadata()));
    }

    @Test(timeout = 60000)
    public void testDecodeDefaultDirect() throws Exception {

        metadataRule.populateDefaultMetadataData();

        DirectBuffer encode = codecRule.getCodec().encodeDirect(metadataRule.getKvMetadata());
        KVMetadata kvMetadata = codecRule.getCodec().decodeDefault(encode, 0);

        MatcherAssert.assertThat("Unexpected decoded metadata.", kvMetadata, mapEqualTo(metadataRule.getKvMetadata()));
    }

}