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

package io.reactivesocket.integration;

import io.reactivesocket.AbstractReactiveSocket;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.client.KeepAliveProvider;
import io.reactivesocket.client.ReactiveSocketClient;
import io.reactivesocket.client.SetupProvider;
import io.reactivesocket.lease.DisabledLeaseAcceptingSocket;
import io.reactivesocket.server.ReactiveSocketServer;
import io.reactivesocket.transport.TransportServer.StartedServer;
import io.reactivesocket.transport.tcp.client.TcpTransportClient;
import io.reactivesocket.transport.tcp.server.TcpTransportServer;
import io.reactivesocket.util.PayloadImpl;
import io.reactivex.Flowable;
import io.reactivex.Single;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.reactivestreams.Publisher;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class IntegrationTest {

    @Rule
    public final ClientServerRule rule = new ClientServerRule();

    @Test(timeout = 2_000L)
    public void testRequest() {
        Flowable.fromPublisher(rule.client.requestResponse(new PayloadImpl("REQUEST", "META")))
                .blockingFirst();
        assertThat("Server did not see the request.", rule.requestCount.get(), is(1));
    }

    @Test(timeout = 2_000L)
    public void testClose() throws ExecutionException, InterruptedException, TimeoutException {
        Flowable.fromPublisher(rule.client.close()).ignoreElements().blockingGet();
        Thread.sleep(100);
        assertThat("Server did not disconnect.", rule.disconnectionCounter.get(), is(1));
    }

    public static class ClientServerRule extends ExternalResource {

        private StartedServer server;
        private ReactiveSocket client;
        private AtomicInteger requestCount;
        private AtomicInteger disconnectionCounter;

        @Override
        public Statement apply(final Statement base, Description description) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    requestCount = new AtomicInteger();
                    disconnectionCounter = new AtomicInteger();
                    server = ReactiveSocketServer.create(TcpTransportServer.create())
                                        .start((setup, sendingSocket) -> {
                                            Flowable.fromPublisher(sendingSocket.onClose())
                                                .doOnTerminate(() -> disconnectionCounter.incrementAndGet())
                                                .subscribe();

                                            return new DisabledLeaseAcceptingSocket(new AbstractReactiveSocket() {
                                                @Override
                                                public Publisher<Payload> requestResponse(Payload payload) {
                                                    return Flowable.<Payload>just(new PayloadImpl("RESPONSE", "METADATA"))
                                                            .doOnSubscribe(s -> requestCount.incrementAndGet());
                                                }
                                            });
                                        });
                    client = Single.fromPublisher(ReactiveSocketClient.create(TcpTransportClient.create(server.getServerAddress()),
                                                                     SetupProvider.keepAlive(KeepAliveProvider.never())
                                                                                .disableLease())
                                                             .connect())
                                   .blockingGet();
                    base.evaluate();
                }
            };
        }
    }

}
