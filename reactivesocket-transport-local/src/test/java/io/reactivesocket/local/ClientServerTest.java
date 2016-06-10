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
package io.reactivesocket.local;

import io.reactivesocket.ConnectionSetupHandler;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.RequestHandler;
import io.reactivesocket.exceptions.SetupException;
import io.reactivesocket.test.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import rx.Observable;
import rx.RxReactiveStreams;
import rx.observers.TestSubscriber;

import java.util.concurrent.TimeUnit;

import static io.reactivesocket.util.Unsafe.toSingleFuture;

public class ClientServerTest {

    static ReactiveSocket client;

    static ReactiveSocket server;

    @BeforeClass
    public static void setup() throws Exception {
        LocalServerReactiveSocketConnector.Config serverConfig = new LocalServerReactiveSocketConnector.Config("test", new ConnectionSetupHandler() {
            @Override
            public RequestHandler apply(ConnectionSetupPayload setupPayload, ReactiveSocket rs) throws SetupException {
                return new RequestHandler() {
                    @Override
                    public Publisher<Payload> handleRequestResponse(Payload payload) {
                        return s -> {
                            Payload response = TestUtil.utf8EncodedPayload("hello world", "metadata");
                            s.onNext(response);
                            s.onComplete();
                        };
                    }

                    @Override
                    public Publisher<Payload> handleRequestStream(Payload payload) {
                        Payload response = TestUtil.utf8EncodedPayload("hello world", "metadata");

                        return RxReactiveStreams
                            .toPublisher(Observable
                                .range(1, 10)
                                .map(i -> response));
                    }

                    @Override
                    public Publisher<Payload> handleSubscription(Payload payload) {
                        Payload response = TestUtil.utf8EncodedPayload("hello world", "metadata");

                        return RxReactiveStreams
                            .toPublisher(Observable
                                .range(1, 10)
                                .map(i -> response)
                                .repeat());
                    }

                    @Override
                    public Publisher<Void> handleFireAndForget(Payload payload) {
                        return Subscriber::onComplete;
                    }

                    @Override
                    public Publisher<Payload> handleChannel(Payload initialPayload, Publisher<Payload> inputs) {
                        return null;
                    }

                    @Override
                    public Publisher<Void> handleMetadataPush(Payload payload) {
                        return null;
                    }
                };
            }
        });

        server = toSingleFuture(LocalServerReactiveSocketConnector.INSTANCE.connect(serverConfig)).get(5, TimeUnit.SECONDS);

        LocalClientReactiveSocketConnector.Config clientConfig = new LocalClientReactiveSocketConnector.Config("test", "text", "text");
        client = toSingleFuture(LocalClientReactiveSocketConnector.INSTANCE.connect(clientConfig)).get(5, TimeUnit.SECONDS);;
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
    public void testRequestResponse100_000() {
        requestResponseN(60_000, 10_000);
    }
    @Test
    public void testRequestResponse1_000_000() {
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
                    .toObservable(client.requestResponse(TestUtil.utf8EncodedPayload("hello", "metadata")))
                    .map(payload -> TestUtil.byteToString(payload.getData()))
            )
            .doOnError(Throwable::printStackTrace)
            .subscribe(ts);

        ts.awaitTerminalEvent(timeout, TimeUnit.MILLISECONDS);
        ts.assertValueCount(count);
        ts.assertNoErrors();
        ts.assertCompleted();
    }


}