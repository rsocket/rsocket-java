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

import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class MetadataRule extends ExternalResource {

    private KVMetadataImpl kvMetadata;

    @Override
    public Statement apply(final Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                kvMetadata = new KVMetadataImpl(new HashMap<>());
                base.evaluate();
            }
        };
    }

    public void populateDefaultMetadataData() {
        addMetadata("Hello1", "HelloVal1");
        addMetadata("Hello2", "HelloVal2");
    }

    public void addMetadata(String key, String value) {
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
        ByteBuffer allocate = ByteBuffer.allocate(valueBytes.length);
        allocate.put(valueBytes).flip();
        kvMetadata.put(key, allocate);
    }

    public void addMetadata(String key, ByteBuffer value) {
        kvMetadata.put(key, value);
    }

    public KVMetadataImpl getKvMetadata() {
        return kvMetadata;
    }

}
