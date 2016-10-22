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
package io.reactivesocket.local;

import io.reactivesocket.lease.DisabledLeaseAcceptingSocket;
import io.reactivesocket.server.ReactiveSocketServer;
import io.reactivesocket.test.ClientSetupRule;
import io.reactivesocket.test.TestReactiveSocket;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class ClientServerTest {

    @Rule
    public final ClientSetupRule setup = new LocalRule();

    @Test(timeout = 10000)
    public void testRequestResponse1() {
        setup.testRequestResponseN(1);
    }

    @Test(timeout = 10000)
    public void testRequestResponse10() {
        setup.testRequestResponseN(10);
    }


    @Test(timeout = 10000)
    public void testRequestResponse100() {
        setup.testRequestResponseN(100);
    }

    @Test(timeout = 10000)
    public void testRequestResponse10_000() {
        setup.testRequestResponseN(10_000);
    }

    @Ignore("Stream/Subscription does not work as of now.")
    @Test(timeout = 10000)
    public void testRequestStream() {
        setup.testRequestStream();
    }

    @Ignore("Stream/Subscription does not work as of now.")
    @Test(timeout = 10000)
    public void testRequestSubscription() throws InterruptedException {
        setup.testRequestSubscription();
    }

    private static class LocalRule extends ClientSetupRule {

        private static final AtomicInteger uniqueNameGenerator = new AtomicInteger();

        public LocalRule() {
            super(socketAddress -> {
                if (socketAddress instanceof LocalSocketAddress) {
                    LocalSocketAddress addr = (LocalSocketAddress) socketAddress;
                    return LocalClient.create(addr.getName());
                }
                throw new IllegalArgumentException("Only " + LocalSocketAddress.class.getName() + " are supported.");
            }, () -> {
                LocalServer localServer = LocalServer.create("test-local-server-"
                                                             + uniqueNameGenerator.incrementAndGet());
                return ReactiveSocketServer.create(localServer)
                                           .start((setup, sendingSocket) -> {
                                               return new DisabledLeaseAcceptingSocket(new TestReactiveSocket());
                                           })
                                           .getServerAddress();
            });
        }
    }
}