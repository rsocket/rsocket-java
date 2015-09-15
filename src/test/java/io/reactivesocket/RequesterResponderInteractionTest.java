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

import static io.reactivesocket.LeaseGovernor.*;
import static io.reactivex.Observable.*;
import static org.junit.Assert.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import io.reactivesocket.internal.Requester;
import io.reactivesocket.internal.Responder;
import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subscribers.TestSubscriber;

public class RequesterResponderInteractionTest
{
    Responder responder;
    Requester requester;

    /**
     * Connect a client/server with protocols on either end.
     * @throws InterruptedException 
     */
    @Before
    public void setup() throws InterruptedException {
        System.out.println("-----------------------------------------------------------------------");
        TestConnection serverConnection = new TestConnection();
        TestConnection clientConnection = new TestConnection();
        clientConnection.connectToServerConnection(serverConnection);
        
        LatchedCompletable lc = new LatchedCompletable(2);
        
        responder = Responder.create(serverConnection, setup -> new RequestHandler.Builder()
            .withRequestResponse(payload ->
            {
                final String requestResponse = TestUtil.byteToString(payload.getData());

                if (requestResponse.equals("hello"))
                {
                    return just(TestUtil.utf8EncodedPayload(requestResponse + " world", null));
                }
                else
                {
                    return error(new Exception("Not Found!"));
                }
            })
            .withRequestStream(payload ->
            {
                final String requestStream = TestUtil.byteToString(payload.getData());

                if (requestStream.equals("range"))
                {
                    return range(0, 3).map(String::valueOf).map(i -> TestUtil.utf8EncodedPayload(i, null));
                }
                else
                {
                    return error(new Exception("Not Found!"));
                }
            })
            .withRequestSubscription(payload ->
            {
                final String requestSubscription = TestUtil.byteToString(payload.getData());

                if (requestSubscription.equals("range"))
                {
                    return range(0, 3).map(String::valueOf).map(i -> TestUtil.utf8EncodedPayload(i, null))
                        .mergeWith(never());// never() so it doesn't complete
                }
                else if (requestSubscription.equals("rangeThatCompletes"))
                {
                    // complete and show it turns into an error
                    return range(0, 2).map(String::valueOf).map(i -> TestUtil.utf8EncodedPayload(i, null));
                }
                else
                {
                    return error(new Exception("Not Found!"));
                }
            })
            .withFireAndForget(payload ->
            {
                final String fireAndForget = TestUtil.byteToString(payload.getData());

                if (fireAndForget.equals("hello"))
                {
                    return empty();// nothing responds
                }
                else
                {
                    return error(new Exception("Not Found!"));
                }
            }).build(), NULL_LEASE_GOVERNOR, Throwable::printStackTrace, lc);

        requester = Requester.createClientRequester(clientConnection, ConnectionSetupPayload.create("UTF-8", "UTF-8"), Throwable::printStackTrace, lc);
        
        lc.await();
    }

    @Ignore
    @Test(timeout=2000)
    public void testRequestResponseSuccess() {
        TestSubscriber<Payload> ts = new TestSubscriber<>();
        requester.requestResponse(TestUtil.utf8EncodedPayload("hello", null)).subscribe(ts);
        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
        ts.assertValue(TestUtil.utf8EncodedPayload("hello world", null));
    }

    @Ignore
    @Test(timeout=2000)
    public void testRequestResponseError() {
        TestSubscriber<Payload> ts = new TestSubscriber<>();
        requester.requestResponse(TestUtil.utf8EncodedPayload("doesntExist", null)).subscribe(ts);
        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS); // TODO this fails non-deterministically
        ts.assertError(Exception.class);
        assertEquals("Not Found!", ts.errors().get(0).getMessage());
    }

    @Ignore
    @Test(timeout=2000)
    public void testRequestStreamSuccess() {
        TestSubscriber<Payload> ts = new TestSubscriber<>();
        requester.requestStream(TestUtil.utf8EncodedPayload("range", null)).subscribe(ts);
        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
        ts.assertValueSequence(Observable.just("0", "1", "2").map(s -> TestUtil.utf8EncodedPayload(s, null)).toList().toBlocking().single());
    }

    @Ignore
    @Test(timeout=2000)
    public void testRequestStreamError() {
        TestSubscriber<Payload> ts = new TestSubscriber<>();
        requester.requestStream(TestUtil.utf8EncodedPayload("doesntExist", null)).subscribe(ts);
        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
        ts.assertError(Exception.class);
        assertEquals("Not Found!", ts.errors().get(0).getMessage());
    }

    @Ignore
    @Test(timeout=2000)
    public void testRequestSubscriptionSuccess() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(3);
        TestSubscriber<Payload> ts = new TestSubscriber<>(new Observer<Payload>() {

            @Override
            public void onComplete() {
                
            }

            @Override
            public void onError(Throwable e) {
                
            }

            @Override
            public void onNext(Payload t) {
                latch.countDown();
            }
            
        });
        requester.requestSubscription(TestUtil.utf8EncodedPayload("range", null)).subscribe(ts);
        // wait for 3 events (but no terminal event)
        latch.await(500, TimeUnit.MILLISECONDS);
        ts.assertValueSequence(Observable.just("0", "1", "2").map(s -> TestUtil.utf8EncodedPayload(s, null)).toList().toBlocking().single());
        ts.assertNotTerminated();
        ts.cancel();
    }

    @Ignore
    @Test(timeout=2000)
    public void testRequestSubscriptionErrorOnCompletion() {
        TestSubscriber<Payload> ts = new TestSubscriber<>();
        requester.requestSubscription(TestUtil.utf8EncodedPayload("rangeThatCompletes", null)).subscribe(ts);
        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS); // TODO this fails non-deterministically
        ts.assertValueSequence(Observable.just("0", "1").map(s -> TestUtil.utf8EncodedPayload(s, null)).toList().toBlocking().single());
        ts.assertError(Exception.class);
        assertEquals("Subscription terminated unexpectedly", ts.errors().get(0).getMessage());
    }

    @Ignore
    @Test(timeout=2000)
    public void testRequestSubscriptionError() {
        TestSubscriber<Payload> ts = new TestSubscriber<>();
        requester.requestStream(TestUtil.utf8EncodedPayload("doesntExist", null)).subscribe(ts);
        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS); // TODO this fails non-deterministically
        ts.assertError(Exception.class);
        assertEquals("Not Found!", ts.errors().get(0).getMessage());
    }

    @Ignore
    @Test(timeout=2000)
    public void testFireAndForgetSuccess() {
        TestSubscriber<Void> ts = new TestSubscriber<>();
        requester.fireAndForget(TestUtil.utf8EncodedPayload("hello", null)).subscribe(ts);
        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
        ts.assertValueCount(0);
        ts.assertNoErrors();
    }

    // this is testing that the client is completely unaware of server-side errors
    @Ignore
    @Test(timeout=2000)
    public void testFireAndForgetError() {
        TestSubscriber<Void> ts = new TestSubscriber<>();
        requester.fireAndForget(TestUtil.utf8EncodedPayload("doesntExist", null)).subscribe(ts);
        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
        ts.assertNoErrors();// because we "forget"
    }

}
