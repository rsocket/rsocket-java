/*
 * Copyright 2016 Netflix, Inc.
 * <p>
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *  <p>
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  <p>
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations under the License.
 */

package io.rsocket.util;

import io.rsocket.TestUtil;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

public class PayloadImplTest {
    public static final String DATA_VAL = "data";
    public static final String METADATA_VAL = "metadata";

    @Test
    public void testReuse() throws Exception {
        PayloadImpl p = new PayloadImpl(DATA_VAL, METADATA_VAL);
        assertDataAndMetadata(p);
        assertDataAndMetadata(p);
    }

    @Test
    public void testReuseWithExternalMark() throws Exception {
        PayloadImpl p = new PayloadImpl(DATA_VAL, METADATA_VAL);
        assertDataAndMetadata(p);
        p.getData().position(2).mark();
        assertDataAndMetadata(p);
    }

    public void assertDataAndMetadata(PayloadImpl p) {
        assertThat("Unexpected data.", TestUtil.byteToString(p.getData()), equalTo(DATA_VAL));
        assertThat("Unexpected metadata.", TestUtil.byteToString(p.getMetadata()), equalTo(METADATA_VAL));
    }
}