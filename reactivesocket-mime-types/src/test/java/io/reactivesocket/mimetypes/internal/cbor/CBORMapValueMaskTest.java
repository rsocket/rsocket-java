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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.Matchers.*;

@RunWith(Parameterized.class)
public class CBORMapValueMaskTest {

    @Parameters
    public static Collection<Integer[]> data() {
        return Arrays.asList(new Integer[][] {
                {0, 0}, {20, 6}, {100, Integer.MAX_VALUE}
        });
    }

    @Parameter
    public int offset;
    @Parameter(1)
    public int length;

    @Test(timeout = 60000)
    public void testMask() throws Exception {
        long mask = CBORMap.encodeValueMask(offset, length);
        int offset = CBORMap.decodeOffsetFromMask(mask);
        int length = CBORMap.decodeLengthFromMask(mask);

        MatcherAssert.assertThat("Unexpected offset post decode.", offset, is(this.offset));
        MatcherAssert.assertThat("Unexpected length post decode.", length, is(this.length));
    }
}