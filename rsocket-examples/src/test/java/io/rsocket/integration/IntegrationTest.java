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

package io.rsocket.integration;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.Plugins;
import io.rsocket.RSocket;
import io.rsocket.client.KeepAliveProvider;
import io.rsocket.client.RSocketClient;
import io.rsocket.client.SetupProvider;
import io.rsocket.lease.DisabledLeaseAcceptingSocket;
import io.rsocket.server.RSocketServer;
import io.rsocket.transport.TransportServer.StartedServer;
import io.rsocket.transport.netty.client.TcpTransportClient;
import io.rsocket.transport.netty.server.TcpTransportServer;
import io.rsocket.util.PayloadImpl;
import io.rsocket.util.RSocketProxy;
import io.reactivex.subscribers.TestSubscriber;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.tcp.TcpClient;
import reactor.ipc.netty.tcp.TcpServer;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class IntegrationTest {

    @Rule
    public final ClientServerRule rule = new ClientServerRule();

    @Test(timeout = 2_000L)
    public void testRequest() {
        rule.client.requestResponse(new PayloadImpl("REQUEST", "META")).block();
        assertThat("Server did not see the request.", rule.requestCount.get(), is(1));
        assertTrue(ClientServerRule.calledClient);
        assertTrue(ClientServerRule.calledServer);
        assertTrue(ClientServerRule.calledFrame);
    }

    @Test
    public void testStream() throws Exception {
        TestSubscriber subscriber = TestSubscriber.create();
        rule
            .client
            .requestStream(new PayloadImpl("start"))
            .subscribe(subscriber);

        subscriber.cancel();
        subscriber.isCancelled();
        subscriber.assertNotComplete();
    }

    @Test(timeout = 3_000L)
    public void testClose() throws ExecutionException, InterruptedException, TimeoutException {

        rule.client.close().block();
        rule.disconnectionCounter.await();
    }

    public static class ClientServerRule extends ExternalResource {

        private StartedServer server;
        private RSocket client;
        private AtomicInteger requestCount;
        private CountDownLatch disconnectionCounter;
        public static volatile boolean calledClient = false;
        public static volatile boolean calledServer = false;
        public static volatile boolean calledFrame = false;

        static {
            Plugins.CLIENT_REACTIVE_SOCKET_INTERCEPTOR = reactiveSocket ->
                Mono.just(new RSocketProxy(reactiveSocket) {
                    @Override
                    public Mono<Payload> requestResponse(Payload payload) {
                        calledClient = true;
                        return reactiveSocket.requestResponse(payload);
                    }
                });

            Plugins.SERVER_REACTIVE_SOCKET_INTERCEPTOR = reactiveSocket ->
                Mono.just(new RSocketProxy(reactiveSocket) {
                    @Override
                    public Mono<Payload> requestResponse(Payload payload) {
                        calledServer = true;
                        return reactiveSocket.requestResponse(payload);
                    }
                });

            Plugins.DUPLEX_CONNECTION_INTERCEPTOR = (type, connection) -> {
                calledFrame = true;
                return connection;
            };
        }

        @Override
        public Statement apply(final Statement base, Description description) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    requestCount = new AtomicInteger();
                    disconnectionCounter = new CountDownLatch(1);
                    server = RSocketServer.create(TcpTransportServer.create(TcpServer.create()))
                        .start((setup, sendingSocket) -> {
                            sendingSocket.onClose()
                                .doFinally(signalType -> disconnectionCounter.countDown())
                                .subscribe();

                            return new DisabledLeaseAcceptingSocket(new AbstractRSocket() {
                                @Override
                                public Mono<Payload> requestResponse(Payload payload) {
                                    return Mono.<Payload>just(new PayloadImpl("RESPONSE", "METADATA"))
                                        .doOnSubscribe(s -> requestCount.incrementAndGet());
                                }

                                @Override
                                public Flux<Payload> requestStream(Payload payload) {
                                    return Flux
                                        .range(1, 10_000)
                                        .map(i -> new PayloadImpl("data -> " + i));
                                }
                            });
                        });
                    client = RSocketClient.create(TcpTransportClient.create(TcpClient.create(options ->
                            options.connect((InetSocketAddress) server.getServerAddress()))),
                        SetupProvider.keepAlive(KeepAliveProvider.never())
                            .disableLease())
                        .connect()
                        .block();
                    base.evaluate();
                }
            };
        }
    }

}
