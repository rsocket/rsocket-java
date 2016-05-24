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

import org.hamcrest.MatcherAssert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.model.Statement;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.Matchers.*;

@RunWith(Parameterized.class)
public class IndexedUnsafeBufferTest {

    @Parameters
    public static Collection<Integer[]> data() {
        return Arrays.asList(new Integer[][] {
                {0, 0, 0},
                {10, 0, 10},
                {100, 0, 100},
                {500, 0, 500},
                {500, 10, 400},
                {500, 10, 490},
        });
    }

    @Parameter
    public int bufferSize;
    @Parameter(1)
    public int bufferOffset;
    @Parameter(2)
    public int bufferLength;

    @Rule
    public final BufferRule bufferRule = new BufferRule();

    @Test(timeout = 60000)
    public void testForEachByteFound() throws Exception {
        testScanForBreak(bufferLength / 2);
    }

    @Test(timeout = 60000)
    public void testForEachByteNotFound() throws Exception {
        bufferRule.initBuffer(bufferSize, bufferOffset, bufferLength);
        int i = bufferRule.buffer.forEachByte(CBORUtils.BREAK_SCANNER);
        MatcherAssert.assertThat("Unexpected index.", i, is(bufferRule.buffer.getBackingBuffer().capacity()));
    }

    @Test(timeout = 60000)
    public void testForEachByteLastByte() throws Exception {
        testScanForBreak(bufferLength - bufferOffset - 1);
    }

    private void testScanForBreak(int indexForBreak) {
        bufferRule.initBuffer(bufferSize, bufferOffset, bufferLength);
        if (bufferSize > 0) {
            bufferRule.buffer.getBackingBuffer().putByte(indexForBreak, CborMajorType.CBOR_BREAK);
        }

        int i = bufferRule.buffer.forEachByte(CBORUtils.BREAK_SCANNER);
        MatcherAssert.assertThat("Unexpected index.", i, is(Math.max(0, indexForBreak)));
    }

    public static class BufferRule extends ExternalResource {

        private IndexedUnsafeBuffer buffer;

        @Override
        public Statement apply(final Statement base, Description description) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    initBuffer(0, 0, 0);
                    base.evaluate();
                }
            };
        }

        public void initBuffer(int size, int offset, int length) {
            ByteBuffer b = ByteBuffer.allocate(size);
            buffer = new IndexedUnsafeBuffer(ByteOrder.BIG_ENDIAN);
            buffer.wrap(b, 0, length - offset);
            if (length != 0) {
                buffer.setReaderIndex(offset);
                buffer.setWriterIndex(offset);
            }
        }
    }
}