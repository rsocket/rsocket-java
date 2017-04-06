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

package io.reactivesocket.test;

import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.client.KeepAliveProvider;
import io.reactivesocket.client.ReactiveSocketClient;
import io.reactivesocket.client.SetupProvider;
import io.reactivesocket.transport.TransportClient;
import io.reactivesocket.util.PayloadImpl;
import io.reactivex.subscribers.TestSubscriber;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import reactor.core.publisher.Flux;

import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import static org.junit.Assert.fail;


public class ClientSetupRule extends ExternalResource {

    private final Callable<SocketAddress> serverStarter;
    private final Function<SocketAddress, TransportClient> clientFactory;
    private SocketAddress serverAddress;
    private ReactiveSocket reactiveSocket;
    private ReactiveSocketClient reactiveSocketClient;

    public ClientSetupRule(Function<SocketAddress, TransportClient> clientFactory, Callable<SocketAddress> serverStarter) {
        this.clientFactory = clientFactory;
        this.serverStarter = serverStarter;
    }

    @Override
    public Statement apply(final Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                serverAddress = serverStarter.call();
                TransportClient client = clientFactory.apply(serverAddress);
                SetupProvider setup = SetupProvider.keepAlive(KeepAliveProvider.never()).disableLease();
                reactiveSocketClient = ReactiveSocketClient.create(client, setup);
                base.evaluate();
            }
        };
    }

    public ReactiveSocketClient getClient() {
        return reactiveSocketClient;
    }

    public SocketAddress getServerAddress() {
        return serverAddress;
    }

    public ReactiveSocket getReactiveSocket() {
        if (null == reactiveSocket) {
            reactiveSocket = reactiveSocketClient.connect().block();
        }
        return reactiveSocket;
    }

    public void testFireAndForget(int count) {
        TestSubscriber<Void> ts = TestSubscriber.create();
        Flux.range(1, count)
            .flatMap(i ->
                getReactiveSocket()
                    .fireAndForget(new PayloadImpl("hello", "metadata"))
            )
            .doOnError(Throwable::printStackTrace)
            .subscribe(ts);

        await(ts);
        ts.assertTerminated();
        ts.assertNoErrors();
        ts.assertTerminated();
    }

    public void testMetadata(int count) {
        TestSubscriber<Void> ts = TestSubscriber.create();
        Flux.range(1, count)
            .flatMap(i ->
                getReactiveSocket()
                    .metadataPush(new PayloadImpl("", "metadata"))
            )
            .doOnError(Throwable::printStackTrace)
            .subscribe(ts);

        await(ts);
        ts.assertTerminated();
        ts.assertNoErrors();
        ts.assertTerminated();
    }

    public void testRequestResponseN(int count) {
        TestSubscriber<String> ts = TestSubscriber.create();
        Flux.range(1, count)
            .flatMap(i ->
                getReactiveSocket()
                .requestResponse(new PayloadImpl("hello", "metadata"))
                .map(payload -> StandardCharsets.UTF_8.decode(payload.getData()).toString())
            )
                .doOnError(Throwable::printStackTrace)
                .subscribe(ts);

        await(ts);
        ts.assertTerminated();
        ts.assertValueCount(count);
        ts.assertNoErrors();
        ts.assertTerminated();
    }

    public void testRequestStream() {
        testStream(socket -> socket.requestStream(new PayloadImpl("hello", "metadata")));
    }

    public void testRequestStreamWithRequestN() {
        testStreamRequestN(socket -> socket.requestStream(new PayloadImpl("hello", "metadata")));
    }


    private void testStreamRequestN(Function<ReactiveSocket, Flux<Payload>> invoker) {
        int count = 10;
        CountDownLatch latch = new CountDownLatch(count);
        TestSubscriber<Payload> ts = TestSubscriber.create(count / 2);
        Flux<Payload> publisher = invoker.apply(getReactiveSocket());
        publisher
            .doOnNext(s -> latch.countDown())
            .subscribe(ts);

        ts.request(count / 2);

        try {
            latch.await();
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }

        ts.assertNoErrors();
        ts.assertValueCount(count);
        ts.assertNotTerminated();
    }

    private void testStream(Function<ReactiveSocket, Flux<Payload>> invoker) {
        TestSubscriber<Payload> ts = TestSubscriber.create();
        Flux<Payload> publisher = invoker.apply(getReactiveSocket());
        publisher
            .take(5)
            .subscribe(ts);
        await(ts);
        ts.assertTerminated();
        ts.assertNoErrors();
        ts.assertValueCount(5);
        ts.assertTerminated();
    }

    private static void await(TestSubscriber<?> ts) {
        try {
            ts.await();
        } catch (InterruptedException e) {
            fail("Interrupted while waiting for completion.");
        }
    }
}
