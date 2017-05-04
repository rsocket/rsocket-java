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

package io.rsocket.examples.transport.tcp.stress;

import io.rsocket.util.PayloadImpl;

import java.time.Duration;
import java.util.function.IntSupplier;

public final class StressTestDriver {

    public static void main(String... args) throws Exception {
        Duration testDuration = Duration.ofMinutes(1);
        int maxConcurrency = 100;
        boolean enableLease = true;
        IntSupplier leaseSupplier = () -> 100_000;
        int leaseTtlMillis = 30_000;

        TestConfig config;
        if (enableLease) {
            config = new TestConfig(testDuration, maxConcurrency, leaseSupplier, leaseTtlMillis);
        } else {
            config = new TestConfig(testDuration, maxConcurrency, enableLease);
        }

        StressTest test = new StressTest(config);

        test.printStatsEvery(Duration.ofSeconds(5))
            .startClient()
            .startTest(reactiveSocket -> reactiveSocket.requestResponse(new PayloadImpl("Hello", "META")));
    }
}
