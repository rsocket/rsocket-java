/**
 * Copyright 2015 Netflix, Inc.
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
package io.reactivesocket.javax.websocket;

import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.DefaultReactiveSocket;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.javax.websocket.client.ReactiveSocketWebSocketClient;
import io.reactivesocket.test.TestUtil;
import org.glassfish.tyrus.client.ClientManager;
import org.glassfish.tyrus.server.Server;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import rx.Observable;
import rx.RxReactiveStreams;
import rx.observers.TestSubscriber;

import javax.websocket.DeploymentException;
import javax.websocket.Endpoint;
import javax.websocket.server.ServerApplicationConfig;
import javax.websocket.server.ServerEndpointConfig;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ClientServerTest {

    static ReactiveSocket client;
    static Server server;

    public static class ApplicationConfig implements ServerApplicationConfig {
        @Override
        public Set<ServerEndpointConfig> getEndpointConfigs(Set<Class<? extends Endpoint>> endpointClasses) {
            Set<ServerEndpointConfig> cfgs = new HashSet<>();
            cfgs.add(ServerEndpointConfig.Builder
                .create(ClientServerEndpoint.class, "/rs")
                .build());
            return cfgs;
        }

        @Override
        public Set<Class<?>> getAnnotatedEndpointClasses(Set<Class<?>> scanned) {
            return Collections.emptySet();
        }
    }

    @BeforeClass
    public static void setup() throws URISyntaxException, DeploymentException, IOException {
        server = new Server("localhost", 8025, null, null, ApplicationConfig.class);
        server.start();

        WebSocketDuplexConnection duplexConnection = RxReactiveStreams.toObservable(
                ReactiveSocketWebSocketClient.create(InetSocketAddress.createUnresolved("localhost", 8025), "/rs", ClientManager.createClient())
        ).toBlocking().single();

        client = DefaultReactiveSocket.fromClientConnection(duplexConnection, ConnectionSetupPayload.create("UTF-8", "UTF-8"), t -> t.printStackTrace());

        client.startAndWait();
    }

    @AfterClass
    public static void tearDown() {
        //server.shutdown();
        server.stop();
    }

    @Test
    public void testRequestResponse1() {
        requestResponseN(1500, 1);
    }

    @Test
    public void testRequestResponse10() {
        requestResponseN(1500, 10);
    }


    @Test
    public void testRequestResponse100() {
        requestResponseN(1500, 100);
    }

    @Test
    public void testRequestResponse10_000() {
        requestResponseN(60_000, 10_000);
    }

    @Test
    public void testRequestStream() {
        TestSubscriber<Payload> ts = TestSubscriber.create();

        RxReactiveStreams
            .toObservable(client.requestStream(TestUtil.utf8EncodedPayload("hello", "metadata")))
            .subscribe(ts);


        ts.awaitTerminalEvent(3_000, TimeUnit.MILLISECONDS);
        ts.assertValueCount(10);
        ts.assertNoErrors();
        ts.assertCompleted();
    }

    @Test
    public void testRequestSubscription() throws InterruptedException {
        TestSubscriber<Payload> ts = TestSubscriber.create();

        RxReactiveStreams
            .toObservable(client.requestSubscription(TestUtil.utf8EncodedPayload("hello sub", "metadata sub")))
            .take(10)
            .subscribe(ts);

        ts.awaitTerminalEvent(3_000, TimeUnit.MILLISECONDS);
        ts.assertValueCount(10);
        ts.assertNoErrors();
    }


    public void requestResponseN(int timeout, int count) {

        TestSubscriber<String> ts = TestSubscriber.create();

        Observable
            .range(1, count)
            .flatMap(i ->
                    RxReactiveStreams
                        .toObservable(
                            client.requestResponse(TestUtil.utf8EncodedPayload("hello", "metadata"))
                        )
                        .map(payload ->
                                TestUtil.byteToString(payload.getData())
                        )
                        //.doOnNext(System.out::println)
            )
            .subscribe(ts);

        ts.awaitTerminalEvent(timeout, TimeUnit.MILLISECONDS);
        ts.assertValueCount(count);
        ts.assertNoErrors();
        ts.assertCompleted();
    }


}
