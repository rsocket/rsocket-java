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
package io.reactivesocket.local;

import org.junit.Rule;
import org.junit.Test;

public class LocalClientServerTest {

    @Rule
    public final LocalClientSetupRule setup = new LocalClientSetupRule();

    @Test(timeout = 60000)
    public void testRequestResponse1() {
        setup.testRequestResponseN(1);
    }

    @Test(timeout = 60000)
    public void testRequestResponse10() {
        setup.testRequestResponseN(10);
    }


    @Test(timeout = 60000)
    public void testRequestResponse100() {
        setup.testRequestResponseN(100);
    }

    @Test(timeout = 60000)
    public void testRequestResponse10_000() {
        setup.testRequestResponseN(10_000);
    }

    @Test(timeout = 60000)
    public void testRequestStream() {
        setup.testRequestStream();
    }

    @Test(timeout = 60000)
    public void testRequestSubscription() throws InterruptedException {
        setup.testRequestSubscription();
    }
}