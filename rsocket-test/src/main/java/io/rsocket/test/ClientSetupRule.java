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

package io.rsocket.test;

import io.reactivex.subscribers.TestSubscriber;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.PayloadImpl;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import reactor.core.publisher.Flux;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.Assert.fail;


public class ClientSetupRule<T> extends ExternalResource {

    private Supplier<T> address;
    private Function<T, RSocket> rSocketClient;
    private Consumer<T> rSocketServer;

    private RSocket client;

    public ClientSetupRule(Supplier<T> address, Function<T, RSocket> rSocketClient, Consumer<T> rSocketServer) {
        this.address = address;
        this.rSocketClient = rSocketClient;
        this.rSocketServer = rSocketServer;
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                rSocketServer.accept(address.get());
                client = rSocketClient.apply(address.get());
                base.evaluate();
            }
        };
    }

    private RSocket getRSocket() {
        return client;
    }

    public void testFireAndForget(int count) {
        TestSubscriber<Void> ts = TestSubscriber.create();
        Flux.range(1, count)
            .flatMap(i ->
                getRSocket()
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
                getRSocket()
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
                getRSocket()
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


    private void testStreamRequestN(Function<RSocket, Flux<Payload>> invoker) {
        int count = 10;
        CountDownLatch latch = new CountDownLatch(count);
        TestSubscriber<Payload> ts = TestSubscriber.create(count / 2);
        Flux<Payload> publisher = invoker.apply(getRSocket());
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

    private void testStream(Function<RSocket, Flux<Payload>> invoker) {
        TestSubscriber<Payload> ts = TestSubscriber.create();
        Flux<Payload> publisher = invoker.apply(getRSocket());
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
