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

import org.agrona.concurrent.UnsafeBuffer;
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

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

@RunWith(Parameterized.class)
public class CBORMapTest {

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
    public final CborMapRule mapRule = new CborMapRule();

    @Test(timeout = 60000)
    public void testGet() throws Exception {
        mapRule.addMockEntries(entrySize);
        for (int i = 0; i < entrySize; i++) {
            mapRule.assertValueForKey(i);
        }
    }

    @Test(timeout = 60000)
    public void testGetWithArrayWrappedBuffer() throws Exception {
        mapRule.initWithArray();
        mapRule.addMockEntries(entrySize);
        for (int i = 0; i < entrySize; i++) {
            mapRule.assertValueForKey(i);
        }
    }

    @Test(timeout = 60000)
    public void testContainsKey() throws Exception {
        mapRule.addMockEntries(entrySize);
        for (int i = 0; i < entrySize; i++) {
            String k = mapRule.getMockKey(i);
            assertThat("Key: " + k + " not found.", mapRule.map.containsKey(k), is(true));
        }
    }

    @Test(timeout = 60000)
    public void testNonExistentContainsKey() throws Exception {
        mapRule.addMockEntries(entrySize);
        String k = "dummy";
        assertThat("Key: " + k + " not found.", mapRule.map.containsKey(k), is(false));
    }

    @Test(timeout = 60000)
    public void testSize() throws Exception {
        mapRule.addMockEntries(entrySize);
        assertThat("Unexpected size.", mapRule.map.size(), is(entrySize));
    }

    @Test(timeout = 60000)
    public void testIsEmpty() throws Exception {
        mapRule.addMockEntries(entrySize);
        assertThat("isEmpty?.", mapRule.map.isEmpty(), is(false));
    }

    @Test(timeout = 60000)
    public void testIsEmptyWithEmpty() throws Exception {
        assertThat("isEmpty?.", mapRule.map.isEmpty(), is(true));
    }

    @Test(timeout = 60000)
    public void testContainsValue() throws Exception {
        mapRule.addMockEntries(entrySize);
        for (int i = 0; i < entrySize; i++) {
            String v = mapRule.getMockValue(i);
            ByteBuffer vBuf = mapRule.getMockValueAsBuffer(i);
            assertThat("Value: " + v + " not found.", mapRule.map.containsValue(vBuf), is(true));
        }
    }

    @Test(timeout = 60000)
    public void testNonExistentValue() throws Exception {
        mapRule.addMockEntries(entrySize);
        ByteBuffer vBuf = mapRule.toValueBuffer("dummy");
        assertThat("Unexpected value found.", mapRule.map.containsValue(vBuf), is(false));
    }

    @Test(timeout = 60000)
    public void testPut() throws Exception {
        mapRule.addMockEntries(entrySize);
        String addnKey1 = "AddnKey1";
        ByteBuffer addnValue1 = mapRule.toValueBuffer("AddnValue1");
        mapRule.map.put(addnKey1, addnValue1);
        for (int i = 0; i < entrySize; i++) {
            mapRule.assertValueForKey(i);
        }
        ByteBuffer vBuf = mapRule.map.get(addnKey1);

        assertThat("Unexpected lookup value for key: " + addnKey1, vBuf, equalTo(addnValue1));
    }

    @Test(timeout = 60000)
    public void testRemove() throws Exception {
        mapRule.addMockEntries(entrySize);
        int indexToRemove = 0 == entrySize? 0 : entrySize - 1;
        String keyToRemove = mapRule.getMockKey(indexToRemove);
        ByteBuffer removed = mapRule.map.remove(keyToRemove);

        assertThat("Unexpected value removed", removed, equalTo(mapRule.getMockValueAsBuffer(indexToRemove)));
        assertThat("Value not removed from map.", mapRule.map.get(keyToRemove), is(nullValue()));
    }

    public class CborMapRule extends AbstractCborMapRule<CBORMap> {

        @Override
        protected void init() {
            valueBuffer = ByteBuffer.allocate(bufferSize);
            indexed = new IndexedUnsafeBuffer(ByteOrder.BIG_ENDIAN);
            indexed.wrap(valueBuffer, bufferOffset, bufferLength);
            map = new CBORMap(indexed.getBackingBuffer(), bufferOffset);
        }

        protected void initWithArray() {
            byte[] src = new byte[bufferSize];
            UnsafeBuffer unsafeBuffer = new UnsafeBuffer(src, bufferOffset, bufferLength);
            indexed = new IndexedUnsafeBuffer(ByteOrder.BIG_ENDIAN);
            indexed.wrap(unsafeBuffer);
            map = new CBORMap(indexed.getBackingBuffer(), bufferOffset);
        }
    }
}