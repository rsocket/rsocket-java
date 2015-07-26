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
package io.reactivesocket;

import static org.junit.Assert.assertEquals;
import static rx.Observable.empty;
import static rx.Observable.error;
import static rx.Observable.just;
import static rx.Observable.never;
import static rx.Observable.range;
import static rx.RxReactiveStreams.toObservable;
import static rx.RxReactiveStreams.toPublisher;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import rx.Observer;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;

public class ClientServerInteractionTest {

    ReactiveSocketServerProtocol serverProtocol;
    ReactiveSocketClientProtocol clientProtocol;

    /**
     * Connect a client/server with protocols on either end.
     */
    @Before
    public void setup() {
        System.out.println("-----------------------------------------------------------------------");
        TestConnection serverConnection = new TestConnection();
        serverConnection.writes.forEach(n -> System.out.println("Writes from server->client: " + n));
        serverConnection.toInput.forEach(n -> System.out.println("Input from client->server: " + n));
        TestConnection clientConnection = new TestConnection();
        clientConnection.writes.forEach(n -> System.out.println("Writes from client->server: " + n));
        clientConnection.toInput.forEach(n -> System.out.println("Input from server->client: " + n));

        // connect the connections (with a Scheduler to simulate async IO)
        clientConnection.writes
                .subscribeOn(Schedulers.computation())
                .observeOn(Schedulers.computation())
                .subscribe(serverConnection.toInput);
        serverConnection.writes.observeOn(Schedulers.computation())
                .subscribeOn(Schedulers.computation())
                .observeOn(Schedulers.computation())
                .subscribe(clientConnection.toInput);

        serverProtocol = ReactiveSocketServerProtocol.create(RequestHandler.create(
                requestResponse -> {
                    if (requestResponse.equals("hello")) {
                        return toPublisher(just(requestResponse + " world"));
                    } else {
                        return toPublisher(error(new Exception("Not Found!")));
                    }
                } ,
                requestStream -> {
                    if (requestStream.equals("range")) {
                        return toPublisher(range(0, 3).map(String::valueOf));
                    } else {
                        return toPublisher(error(new Exception("Not Found!")));
                    }
                } ,
                requestSubscription -> {
                    if (requestSubscription.equals("range")) {
                        return toPublisher(range(0, 3).map(String::valueOf)
                                .mergeWith(never()));// never() so it doesn't complete
                    } else if (requestSubscription.equals("rangeThatCompletes")) {
                        // complete and show it turns into an error
                        return toPublisher(range(0, 2).map(String::valueOf));
                    } else {
                        return toPublisher(error(new Exception("Not Found!")));
                    }
                } ,
                fireAndForget -> {
                    if (fireAndForget.equals("hello")) {
                        return toPublisher(empty());// nothing responds
                    } else {
                        return toPublisher(error(new Exception("Not Found!")));
                    }
                }));

        toObservable(serverProtocol.acceptConnection(serverConnection)).subscribe();// start handling the connection
        clientProtocol = ReactiveSocketClientProtocol.create(clientConnection);
    }

    @Ignore
    @Test
    public void testRequestResponseSuccess() {
        TestSubscriber<String> ts = TestSubscriber.create();
        toObservable(clientProtocol.requestResponse("hello")).subscribe(ts);
        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
        ts.assertValue("hello world");
    }

    @Ignore
    @Test
    public void testRequestResponseError() {
        TestSubscriber<String> ts = TestSubscriber.create();
        toObservable(clientProtocol.requestResponse("doesntExist")).subscribe(ts);
        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS); // TODO this fails non-deterministically
        ts.assertError(Exception.class);
        assertEquals("Not Found!", ts.getOnErrorEvents().get(0).getMessage());
    }

    @Ignore
    @Test
    public void testRequestStreamSuccess() {
        TestSubscriber<String> ts = TestSubscriber.create();
        toObservable(clientProtocol.requestStream("range")).subscribe(ts);
        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
        ts.assertReceivedOnNext(Arrays.asList("0", "1", "2"));
    }

    @Ignore
    @Test
    public void testRequestStreamError() {
        TestSubscriber<String> ts = TestSubscriber.create();
        toObservable(clientProtocol.requestStream("doesntExist")).subscribe(ts);
        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
        ts.assertError(Exception.class);
        assertEquals("Not Found!", ts.getOnErrorEvents().get(0).getMessage());
    }

    @Ignore
    @Test
    public void testRequestSubscriptionSuccess() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(3);
        TestSubscriber<String> ts = TestSubscriber.create(new Observer<String>() {

            @Override
            public void onCompleted() {
                
            }

            @Override
            public void onError(Throwable e) {
                
            }

            @Override
            public void onNext(String t) {
                latch.countDown();
            }
            
        });
        toObservable(clientProtocol.requestSubscription("range")).subscribe(ts);
        // wait for 3 events (but no terminal event)
        latch.await(500, TimeUnit.MILLISECONDS);
        ts.assertReceivedOnNext(Arrays.asList("0", "1", "2"));
        ts.assertNoTerminalEvent();
        ts.unsubscribe();
    }

    @Ignore
    @Test
    public void testRequestSubscriptionErrorOnCompletion() {
        TestSubscriber<String> ts = TestSubscriber.create();
        toObservable(clientProtocol.requestSubscription("rangeThatCompletes")).subscribe(ts);
        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS); // TODO this fails non-deterministically
        ts.assertReceivedOnNext(Arrays.asList("0", "1"));
        ts.assertError(Exception.class);
        assertEquals("Subscription terminated unexpectedly", ts.getOnErrorEvents().get(0).getMessage());
    }

    @Ignore
    @Test
    public void testRequestSubscriptionError() {
        TestSubscriber<String> ts = TestSubscriber.create();
        toObservable(clientProtocol.requestStream("doesntExist")).subscribe(ts);
        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS); // TODO this fails non-deterministically
        ts.assertError(Exception.class);
        assertEquals("Not Found!", ts.getOnErrorEvents().get(0).getMessage());
    }

    @Ignore
    @Test
    public void testFireAndForgetSuccess() {
        TestSubscriber<Void> ts = TestSubscriber.create();
        toObservable(clientProtocol.fireAndForget("hello")).subscribe(ts);
        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
        ts.assertValueCount(0);
        ts.assertNoErrors();
    }

    // this is testing that the client is completely unaware of server-side errors
    @Ignore
    @Test
    public void testFireAndForgetError() {
        TestSubscriber<Void> ts = TestSubscriber.create();
        toObservable(clientProtocol.fireAndForget("doesntExist")).subscribe(ts);
        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
        ts.assertNoErrors();// because we "forget"
    }

}
