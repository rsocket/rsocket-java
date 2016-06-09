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

package io.reactivesocket.mimetypes;

import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.mimetypes.internal.Codec;
import io.reactivesocket.mimetypes.internal.CustomObject;
import io.reactivesocket.mimetypes.internal.CustomObjectRule;
import io.reactivesocket.mimetypes.internal.KVMetadataImpl;
import io.reactivesocket.mimetypes.internal.MetadataRule;
import io.reactivesocket.mimetypes.internal.cbor.CborCodec;
import io.reactivesocket.mimetypes.internal.cbor.ReactiveSocketDefaultMetadataCodec;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;

import java.nio.ByteBuffer;

import static io.reactivesocket.mimetypes.SupportedMimeTypes.*;
import static io.reactivesocket.mimetypes.internal.cbor.ByteBufferMapMatcher.*;

public class MimeTypeFactoryTest {

    @Rule
    public final CustomObjectRule objectRule = new CustomObjectRule();
    @Rule
    public final MetadataRule metadataRule = new MetadataRule();

    @Test(timeout = 60000)
    public void testFromSetup() throws Exception {
        objectRule.populateDefaultData();
        metadataRule.populateDefaultMetadataData();

        MimeType mimeType = getMimeTypeFromSetup(ReactiveSocketDefaultMetadata, CBOR);

        testMetadataCodec(mimeType, ReactiveSocketDefaultMetadataCodec.create());
        testDataCodec(mimeType, CborCodec.create());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnsupportedMimetype() throws Exception {
        ConnectionSetupPayload setup = new ConnectionSetupPayloadImpl("blah", "blah");
        MimeTypeFactory.from(setup);
    }

    @Test(timeout = 60000)
    public void testOneMimetype() throws Exception {
        objectRule.populateDefaultData();
        metadataRule.populateDefaultMetadataData();

        MimeType mimeType = MimeTypeFactory.from(CBOR);

        testMetadataCodec(mimeType, CborCodec.create());
        testDataCodec(mimeType, CborCodec.create());
    }

    @Test(timeout = 60000)
    public void testDifferentMimetypes() throws Exception {
        objectRule.populateDefaultData();
        metadataRule.populateDefaultMetadataData();

        MimeType mimeType = MimeTypeFactory.from(ReactiveSocketDefaultMetadata, CBOR);

        testMetadataCodec(mimeType, ReactiveSocketDefaultMetadataCodec.create());
        testDataCodec(mimeType, ReactiveSocketDefaultMetadataCodec.create());
    }

    private void testMetadataCodec(MimeType mimeType, Codec expectedCodec) {
        ByteBuffer encode = mimeType.encodeMetadata(metadataRule.getKvMetadata());
        ByteBuffer encode1 = expectedCodec.encode(metadataRule.getKvMetadata());

        MatcherAssert.assertThat("Unexpected encode from mime type.", encode, Matchers.equalTo(encode1));
        MatcherAssert.assertThat("Unexpected decode from encode.", metadataRule.getKvMetadata(),
                                 mapEqualTo(mimeType.decodeMetadata(encode, KVMetadataImpl.class)));


        DirectBuffer dencode = mimeType.encodeMetadataDirect(metadataRule.getKvMetadata());
        DirectBuffer dencode1 = expectedCodec.encodeDirect(metadataRule.getKvMetadata());

        MatcherAssert.assertThat("Unexpected direct buffer encode from mime type.", dencode, Matchers.equalTo(dencode1));
        MatcherAssert.assertThat("Unexpected decode from direct encode.", metadataRule.getKvMetadata(),
                                 mapEqualTo(mimeType.decodeMetadata(dencode, KVMetadataImpl.class, 0)));

        ByteBuffer dst = ByteBuffer.allocate(100);
        ByteBuffer dst1 = ByteBuffer.allocate(100);

        mimeType.encodeMetadataTo(dst, metadataRule.getKvMetadata());
        dst.flip();
        expectedCodec.encodeTo(dst1, metadataRule.getKvMetadata());
        dst1.flip();

        MatcherAssert.assertThat("Unexpected encodeTo buffer encode from mime type.", dst, Matchers.equalTo(dst1));
        MatcherAssert.assertThat("Unexpected decode from encodeTo encode.", metadataRule.getKvMetadata(),
                                 mapEqualTo(mimeType.decodeMetadata(dst, KVMetadataImpl.class)));

        MutableDirectBuffer mdst = new UnsafeBuffer(new byte[100]);
        MutableDirectBuffer mdst1 = new UnsafeBuffer(new byte[100]);

        mimeType.encodeMetadataTo(mdst, metadataRule.getKvMetadata(), 0);
        expectedCodec.encodeTo(mdst1, metadataRule.getKvMetadata(), 0);

        MatcherAssert.assertThat("Unexpected encodeTo buffer encode from mime type.", mdst, Matchers.equalTo(mdst1));
        MatcherAssert.assertThat("Unexpected decode from encodeTo encode.", metadataRule.getKvMetadata(),
                                 mapEqualTo(mimeType.decodeMetadata(mdst, KVMetadataImpl.class, 0)));
    }

    private void testDataCodec(MimeType mimeType, Codec expectedCodec) {
        ByteBuffer encode = mimeType.encodeData(objectRule.getData());
        ByteBuffer encode1 = expectedCodec.encode(objectRule.getData());

        MatcherAssert.assertThat("Unexpected encode from mime type.", encode, Matchers.equalTo(encode1));
        MatcherAssert.assertThat("Unexpected decode from encode.", objectRule.getData(),
                                 Matchers.equalTo(mimeType.decodeData(encode, CustomObject.class)));


        DirectBuffer dencode = mimeType.encodeDataDirect(objectRule.getData());
        DirectBuffer dencode1 = expectedCodec.encodeDirect(objectRule.getData());

        MatcherAssert.assertThat("Unexpected direct buffer encode from mime type.", dencode, Matchers.equalTo(dencode1));
        MatcherAssert.assertThat("Unexpected decode from direct encode.", objectRule.getData(),
                                 Matchers.equalTo(mimeType.decodeData(dencode, CustomObject.class, 0)));

        ByteBuffer dst = ByteBuffer.allocate(100);
        ByteBuffer dst1 = ByteBuffer.allocate(100);

        mimeType.encodeDataTo(dst, objectRule.getData());
        dst.flip();
        expectedCodec.encodeTo(dst1, objectRule.getData());
        dst1.flip();

        MatcherAssert.assertThat("Unexpected encodeTo buffer encode from mime type.", dst, Matchers.equalTo(dst1));
        MatcherAssert.assertThat("Unexpected decode from encodeTo encode.", objectRule.getData(),
                                 Matchers.equalTo(mimeType.decodeData(dst, CustomObject.class)));

        MutableDirectBuffer mdst = new UnsafeBuffer(new byte[100]);
        MutableDirectBuffer mdst1 = new UnsafeBuffer(new byte[100]);

        mimeType.encodeDataTo(mdst, objectRule.getData(), 0);
        expectedCodec.encodeTo(mdst1, objectRule.getData(), 0);

        MatcherAssert.assertThat("Unexpected encodeTo buffer encode from mime type.", mdst, Matchers.equalTo(mdst1));
        MatcherAssert.assertThat("Unexpected decode from encodeTo encode.", objectRule.getData(),
                                 Matchers.equalTo(mimeType.decodeData(mdst, CustomObject.class, 0)));
    }

    private static MimeType getMimeTypeFromSetup(SupportedMimeTypes metaMime, SupportedMimeTypes dataMime) {
        ConnectionSetupPayload setup = new ConnectionSetupPayloadImpl(dataMime.getMimeTypes().get(0),
                                                                      metaMime.getMimeTypes().get(0));

        return MimeTypeFactory.from(setup);
    }

    private static class ConnectionSetupPayloadImpl extends ConnectionSetupPayload {

        private final String dataMime;
        private final String metadataMime;

        private ConnectionSetupPayloadImpl(String dataMime, String metadataMime) {
            this.dataMime = dataMime;
            this.metadataMime = metadataMime;
        }

        @Override
        public String metadataMimeType() {
            return metadataMime;
        }

        @Override
        public String dataMimeType() {
            return dataMime;
        }

        @Override
        public ByteBuffer getData() {
            return ByteBuffer.allocate(0);
        }

        @Override
        public ByteBuffer getMetadata() {
            return ByteBuffer.allocate(0);
        }
    }
}