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

import org.hamcrest.MatcherAssert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.*;

public class KVMetadataImplTest {

    @Rule
    public final MetadataRule metadataRule = new MetadataRule();

    @Test(timeout = 60000)
    public void testGetAsString() throws Exception {
        String key = "Key1";
        String value = "Value1";
        metadataRule.addMetadata(key, value);

        String lookup = metadataRule.getKvMetadata().getAsString(key, StandardCharsets.UTF_8);

        MatcherAssert.assertThat("Unexpected lookup value.", lookup, equalTo(value));
    }

    @Test(timeout = 60000)
    public void testGetAsEmptyString() throws Exception {
        String key = "Key1";
        String value = "";
        metadataRule.addMetadata(key, value);

        String lookup = metadataRule.getKvMetadata().getAsString(key, StandardCharsets.UTF_8);

        MatcherAssert.assertThat("Unexpected lookup value.", lookup, equalTo(value));
    }

    @Test(timeout = 60000)
    public void testGetAsStringInvalid() throws Exception {

        String lookup = metadataRule.getKvMetadata().getAsString("Key", StandardCharsets.UTF_8);

        MatcherAssert.assertThat("Unexpected lookup value.", lookup, nullValue());
    }
}