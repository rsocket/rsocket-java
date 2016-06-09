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

import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

public abstract class AbstractCborMapRule<T extends CBORMap> extends ExternalResource {

    protected T map;
    protected ByteBuffer valueBuffer;
    protected IndexedUnsafeBuffer indexed;

    @Override
    public Statement apply(final Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                init();
                base.evaluate();
            }
        };
    }

    protected abstract void init();

    public void addMockEntries(int count) {
        for (int i =0; i < count; i++) {
            addEntry(getMockKey(i), getMockValue(i));
        }
    }

    public void addEntry(String utf8Key, String value) {
        ByteBuffer vBuf = toValueBuffer(value);
        int valueLength = vBuf.remaining();
        int offset = indexed.getWriterIndex();
        indexed.writeBytes(vBuf, valueLength);
        map.putValueOffset(utf8Key, offset, valueLength);
    }

    public String getMockKey(int index) {
        return "Key" + index;
    }

    public String getMockValue(int index) {
        return "Value" + (index + 10);
    }

    public ByteBuffer getMockValueAsBuffer(int index) {
        String mockValue = getMockValue(index);
        return toValueBuffer(mockValue);
    }

    public ByteBuffer toValueBuffer(String value) {
        byte[] vBytes = value.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.wrap(vBytes);
    }

    public void assertValueForKey(int index) {
        String k = getMockKey(index);
        ByteBuffer vBuf = map.get(k);

        assertThat("Unexpected lookup value for key: " + k, vBuf, equalTo(getMockValueAsBuffer(index)));
    }
}
