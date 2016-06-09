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
import org.agrona.concurrent.UnsafeBuffer;
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

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

@RunWith(Parameterized.class)
public class SlicedBufferKVMetadataTest {

    @Parameters
    public static Collection<Integer[]> data() {
        return Arrays.asList(new Integer[][] {
                {100, 0, 100, 5}, {400, 20, 200, 20}, {400, 20, 140, 20}
        });
    }

    @Parameter
    public int bufferSize;
    @Parameter(1)
    public int bufferOffset;
    @Parameter(2)
    public int bufferLength;
    @Parameter(3)
    public int entrySize;

    @Rule
    public final SlicedBufferKVMetadataRule mapRule = new SlicedBufferKVMetadataRule();

    @Test(timeout = 60000)
    public void getAsString() throws Exception {
        mapRule.addMockEntries(entrySize);
        for (int i = 0; i < entrySize; i++) {
            mapRule.assertValueAsStringForKey(i);
        }
    }

    @Test(timeout = 60000)
    public void duplicate() throws Exception {
        mapRule.addMockEntries(entrySize);
        KVMetadata duplicate = mapRule.map.duplicate(capacity -> new UnsafeBuffer(ByteBuffer.allocate(capacity)));

        assertThat("Unexpected type of duplicate.", duplicate, instanceOf(SlicedBufferKVMetadata.class));

        SlicedBufferKVMetadata dup = (SlicedBufferKVMetadata) duplicate;

        for (int i = 0; i < entrySize; i++) {
            mapRule.assertValueAsStringForKey(i, dup);
        }
    }

    public class SlicedBufferKVMetadataRule extends AbstractCborMapRule<SlicedBufferKVMetadata> {

        @Override
        protected void init() {
            valueBuffer = ByteBuffer.allocate(bufferSize);
            indexed = new IndexedUnsafeBuffer(ByteOrder.BIG_ENDIAN);
            indexed.wrap(valueBuffer, bufferOffset, bufferLength);
            map = new SlicedBufferKVMetadata(indexed.getBackingBuffer(), bufferOffset);
        }

        public void assertValueAsStringForKey(int index) {
            assertValueAsStringForKey(index, map);
        }

        public void assertValueAsStringForKey(int index, SlicedBufferKVMetadata map) {
            String k = getMockKey(index);
            assertThat("Unexpected lookup value for key: " + k, map.getAsString(k, StandardCharsets.UTF_8),
                       equalTo(getMockValue(index)));
        }
    }

}