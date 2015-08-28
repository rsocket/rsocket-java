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
import static io.reactivex.Observable.*;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import io.reactivesocket.internal.Requester;
import io.reactivesocket.internal.Responder;
import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subscribers.TestSubscriber;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;

public class RequesterResponderInteractionTest
{
    Responder responder;
    Requester requester;

    /**
     * Connect a client/server with protocols on either end.
     */
    @SuppressWarnings("resource")
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


        responder = Responder.create(serverConnection, setup -> RequestHandler.create(
            requestResponsePayload -> {
                final String requestResponse = TestUtil.byteToString(requestResponsePayload.getData());

                if (requestResponse.equals("hello"))
                {
                    return just(TestUtil.utf8EncodedPayload(requestResponse + " world", null));
                }
                else
                {
                    return error(new Exception("Not Found!"));
                }
            },
            requestStreamPayload -> {
                final String requestStream = TestUtil.byteToString(requestStreamPayload.getData());

                if (requestStream.equals("range"))
                {
                    return range(0, 3).map(String::valueOf).map(i -> TestUtil.utf8EncodedPayload(i, null));
                }
                else
                {
                    return error(new Exception("Not Found!"));
                }
            },
            requestSubscriptionPayload -> {
                final String requestSubscription = TestUtil.byteToString(requestSubscriptionPayload.getData());

                if (requestSubscription.equals("range"))
                {
                    return range(0, 3).map(String::valueOf).map(i -> TestUtil.utf8EncodedPayload(i, null)).mergeWith(never());// never() so it doesn't complete
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
            },
            fireAndForgetPayload -> {
                final String fireAndForget = TestUtil.byteToString(fireAndForgetPayload.getData());

                if (fireAndForget.equals("hello"))
                {
                    return empty();// nothing responds
                }
                else
                {
                    return error(new Exception("Not Found!"));
                }
            }), err -> err.printStackTrace());

        requester = Requester.createClientRequester(clientConnection, ConnectionSetupPayload.create("UTF-8", "UTF-8"), err -> err.printStackTrace());
    }

    @Ignore
    @Test
    public void testRequestResponseSuccess() throws InterruptedException {
        TestSubscriber<Payload> ts = new TestSubscriber<>();
        fromPublisher(requester.requestResponse(TestUtil.utf8EncodedPayload("hello", null))).subscribe(ts);
        ts.await(1000, TimeUnit.MILLISECONDS);
        ts.assertValue(TestUtil.utf8EncodedPayload("hello world", null));
    }

    @Ignore
    @Test
    public void testRequestResponseError() throws InterruptedException {
        TestSubscriber<Payload> ts = new TestSubscriber<>();
        fromPublisher(requester.requestResponse(TestUtil.utf8EncodedPayload("doesntExist", null))).subscribe(ts);
        ts.await(1000, TimeUnit.MILLISECONDS); // TODO this fails non-deterministically
        ts.assertError(Exception.class);
        assertEquals("Not Found!", ts.errors().get(0).getMessage());
    }

    @Ignore
    @Test
    public void testRequestStreamSuccess() throws InterruptedException {
        TestSubscriber<Payload> ts = new TestSubscriber<>();
        fromPublisher(requester.requestStream(TestUtil.utf8EncodedPayload("range", null))).subscribe(ts);
        ts.await(1000, TimeUnit.MILLISECONDS);
        ts.assertValueSequence(Arrays.asList("0", "1", "2").stream().map(s -> TestUtil.utf8EncodedPayload(s, null)).collect(Collectors.toList()));
    }

    @Ignore
    @Test
    public void testRequestStreamError() throws InterruptedException {
        TestSubscriber<Payload> ts = new TestSubscriber<>();
        fromPublisher(requester.requestStream(TestUtil.utf8EncodedPayload("doesntExist", null))).subscribe(ts);
        ts.await(1000, TimeUnit.MILLISECONDS);
        ts.assertError(Exception.class);
        assertEquals("Not Found!", ts.errors().get(0).getMessage());
    }

    @Ignore
    @Test
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
        fromPublisher(requester.requestSubscription(TestUtil.utf8EncodedPayload("range", null))).subscribe(ts);
        // wait for 3 events (but no terminal event)
        latch.await(1000, TimeUnit.MILLISECONDS);
        ts.assertValueSequence(Arrays.asList("0", "1", "2").stream().map(s -> TestUtil.utf8EncodedPayload(s, null)).collect(Collectors.toList()));
        ts.assertNotTerminated();
        ts.cancel();
    }

    @Ignore
    @Test
    public void testRequestSubscriptionErrorOnCompletion() throws InterruptedException {
        TestSubscriber<Payload> ts = new TestSubscriber<>();
        fromPublisher(requester.requestSubscription(TestUtil.utf8EncodedPayload("rangeThatCompletes", null))).subscribe(ts);
        ts.await(1000, TimeUnit.MILLISECONDS); // TODO this fails non-deterministically
        ts.assertValueSequence(Arrays.asList("0", "1").stream().map(s -> TestUtil.utf8EncodedPayload(s, null)).collect(Collectors.toList()));
        ts.assertError(Exception.class);
        assertEquals("Subscription terminated unexpectedly", ts.errors().get(0).getMessage());
    }

    @Ignore
    @Test
    public void testRequestSubscriptionError() throws InterruptedException {
        TestSubscriber<Payload> ts = new TestSubscriber<>();
        fromPublisher(requester.requestStream(TestUtil.utf8EncodedPayload("doesntExist", null))).subscribe(ts);
        ts.await(1000, TimeUnit.MILLISECONDS); // TODO this fails non-deterministically
        ts.assertError(Exception.class);
        assertEquals("Not Found!", ts.errors().get(0).getMessage());
    }

    @Ignore
    @Test
    public void testFireAndForgetSuccess() throws InterruptedException {
        TestSubscriber<Void> ts = new TestSubscriber<>();
        fromPublisher(requester.fireAndForget(TestUtil.utf8EncodedPayload("hello", null))).subscribe(ts);
        ts.await(1000, TimeUnit.MILLISECONDS);
        ts.assertValueCount(0);
        ts.assertNoError();
    }

    // this is testing that the client is completely unaware of server-side errors
    @Ignore
    @Test
    public void testFireAndForgetError() throws InterruptedException {
        TestSubscriber<Void> ts = new TestSubscriber<>();
        fromPublisher(requester.fireAndForget(TestUtil.utf8EncodedPayload("doesntExist", null))).subscribe(ts);
        ts.await(1000, TimeUnit.MILLISECONDS);
        ts.assertNoError();// because we "forget"
    }

}
