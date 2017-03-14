/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket.aeron;

import io.reactivesocket.test.ClientSetupRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

@Ignore
public class ClientServerTest {

    @Rule
    public final ClientSetupRule setup = new AeronClientSetupRule();

    @Test(timeout = 10000)
    public void testFireNForget10() {
        setup.testFireAndForget(10);
    }

    @Test(timeout = 10000)
    public void testPushMetadata10() {
        setup.testMetadata(10);
    }

    @Test(timeout = 5000000)
    public void testRequestResponse1() {
        setup.testRequestResponseN(1);
    }

    @Test(timeout = 2000)
    public void testRequestResponse10() {
        setup.testRequestResponseN(10);
    }


    @Test(timeout = 2000)
    public void testRequestResponse100() {
        setup.testRequestResponseN(100);
    }

    @Test(timeout = 5000)
    public void testRequestResponse10_000() {
        setup.testRequestResponseN(10_000);
    }

    @Test(timeout = 10000)
    public void testRequestStream() {
        setup.testRequestStream();
    }
}