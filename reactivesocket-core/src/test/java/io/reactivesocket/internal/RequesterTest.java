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
package io.reactivesocket.internal;

import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;
import io.reactivesocket.LatchedCompletable;
import io.reactivesocket.Payload;
import io.reactivesocket.TestConnection;
import io.reactivesocket.exceptions.InvalidRequestException;
import io.reactivesocket.util.PayloadImpl;
import io.reactivex.Observable;
import io.reactivex.subjects.ReplaySubject;
import io.reactivex.subscribers.TestSubscriber;
import org.hamcrest.MatcherAssert;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.reactivesocket.ConnectionSetupPayload.*;
import static io.reactivesocket.TestUtil.*;
import static io.reactivex.Observable.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class RequesterTest
{
	final static Consumer<Throwable> ERROR_HANDLER = Throwable::printStackTrace;

	@Test(timeout=2000)
    public void testReqRespCancelBeforeRequestN() throws InterruptedException {
        Requester p = createClientRequester();
        testCancelBeforeRequestN(p.requestResponse(utf8EncodedPayload("hello", null)));
    }

    @Test(timeout=2000)
    public void testReqSubscriptionCancelBeforeRequestN() throws InterruptedException {
        Requester p = createClientRequester();
        testCancelBeforeRequestN(p.requestSubscription(utf8EncodedPayload("hello", null)));
    }

    @Test(timeout=2000)
    public void testReqStreamCancelBeforeRequestN() throws InterruptedException {
        Requester p = createClientRequester();
        testCancelBeforeRequestN(p.requestStream(utf8EncodedPayload("hello", null)));
    }

    @Test(timeout=2000)
    public void testReqChannelCancelBeforeRequestN() throws InterruptedException {
        Requester p = createClientRequester();
        testCancelBeforeRequestN(p.requestChannel(just(utf8EncodedPayload("hello", null))));
    }

    @Test(timeout=2000)
    public void testReqFnFCancelBeforeRequestN() throws InterruptedException {
        Requester p = createClientRequester();
        testCancelBeforeRequestN(p.fireAndForget(utf8EncodedPayload("hello", null)));
    }

    @Test(timeout=2000)
    public void testReqMetaPushCancelBeforeRequestN() throws InterruptedException {
        Requester p = createClientRequester();
        testCancelBeforeRequestN(p.metadataPush(utf8EncodedPayload("hello", null)));
    }

    @Test()
    public void testReqStreamRequestLongMax() throws InterruptedException {
        TestConnection testConnection = establishConnection();
        Requester p = createClientRequester(testConnection);

        testRequestLongMaxValue(p.requestStream(new PayloadImpl("")), testConnection);
    }

    @Test()
    public void testReqSubscriptionRequestLongMax() throws InterruptedException {
        TestConnection testConnection = establishConnection();
        Requester p = createClientRequester(testConnection);

        testRequestLongMaxValue(p.requestSubscription(new PayloadImpl("")), testConnection);
    }

    @Test()
    public void testReqChannelRequestLongMax() throws InterruptedException {
        TestConnection testConnection = establishConnection();
        Requester p = createClientRequester(testConnection);

        testRequestLongMaxValue(p.requestChannel(Publishers.just(new PayloadImpl(""))), testConnection);
    }

    @Test(timeout=2000)
    public void testRequestResponseSuccess() throws InterruptedException {
        TestConnection conn = establishConnection();
        ReplaySubject<Frame> requests = captureRequests(conn);
        LatchedCompletable rc = new LatchedCompletable(1);
        Requester p = Requester.createClientRequester(conn, ConnectionSetupPayload.create("UTF-8", "UTF-8", NO_FLAGS), ERROR_HANDLER, rc);
        rc.await();

        TestSubscriber<Payload> ts = new TestSubscriber<>();
        p.requestResponse(utf8EncodedPayload("hello", null)).subscribe(ts);

        ts.assertNoErrors();
        assertEquals(2, requests.getValues().length);
        List<Frame> requested = requests.take(2).toList().toBlocking().single();

        Frame one = requested.get(0);
        assertEquals(0, one.getStreamId());// SETUP always happens on 0
        assertEquals("", byteToString(one.getData()));
        assertEquals(FrameType.SETUP, one.getType());

        Frame two = requested.get(1);
        assertEquals(2, two.getStreamId());// need to start at 2, not 0
        assertEquals("hello", byteToString(two.getData()));
        assertEquals(FrameType.REQUEST_RESPONSE, two.getType());

        // now emit a response to ensure the Publisher receives and completes
        conn.toInput.send(utf8EncodedResponseFrame(2, FrameType.NEXT_COMPLETE, "world"));

        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
        ts.assertValue(utf8EncodedPayload("world", null));
        ts.assertComplete();
    }

	@Test(timeout=2000)
    public void testRequestResponseError() throws InterruptedException {
        TestConnection conn = establishConnection();
        ReplaySubject<Frame> requests = captureRequests(conn);
        LatchedCompletable rc = new LatchedCompletable(1);
        Requester p = Requester.createClientRequester(conn, ConnectionSetupPayload.create("UTF-8", "UTF-8", NO_FLAGS), ERROR_HANDLER, rc);
        rc.await();

        TestSubscriber<Payload> ts = new TestSubscriber<>();
        p.requestResponse(utf8EncodedPayload("hello", null)).subscribe(ts);

        assertEquals(2, requests.getValues().length);
        List<Frame> requested = requests.take(2).toList().toBlocking().single();

        Frame one = requested.get(0);
        assertEquals(0, one.getStreamId());// SETUP always happens on 0
        assertEquals("", byteToString(one.getData()));
        assertEquals(FrameType.SETUP, one.getType());
        
        Frame two = requested.get(1);
        assertEquals(2, two.getStreamId());// need to start at 2, not 0
        assertEquals("hello", byteToString(two.getData()));
        assertEquals(FrameType.REQUEST_RESPONSE, two.getType());

        conn.toInput.send(Frame.Error.from(2, new RuntimeException("Failed")));
        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
        ts.assertError(InvalidRequestException.class);
        assertEquals("Failed", ts.errors().get(0).getMessage());
    }

	@Test(timeout=2000)
    public void testRequestResponseCancel() throws InterruptedException {
        TestConnection conn = establishConnection();
        ReplaySubject<Frame> requests = captureRequests(conn);
        LatchedCompletable rc = new LatchedCompletable(1);
        Requester p = Requester.createClientRequester(conn, ConnectionSetupPayload.create("UTF-8", "UTF-8", NO_FLAGS), ERROR_HANDLER, rc);
        rc.await();

        TestSubscriber<Payload> ts = new TestSubscriber<>();
        p.requestResponse(utf8EncodedPayload("hello", null)).subscribe(ts);
        ts.cancel();

        assertEquals(3, requests.getValues().length);
        List<Frame> requested = requests.take(3).toList().toBlocking().single();

        Frame one = requested.get(0);
        assertEquals(0, one.getStreamId());// SETUP always happens on 0
        assertEquals("", byteToString(one.getData()));
        assertEquals(FrameType.SETUP, one.getType());
        
        Frame two = requested.get(1);
        assertEquals(2, two.getStreamId());// need to start at 2, not 0
        assertEquals("hello", byteToString(two.getData()));
        assertEquals(FrameType.REQUEST_RESPONSE, two.getType());

        Frame three = requested.get(2);
        assertEquals(2, three.getStreamId());// still the same stream
        assertEquals("", byteToString(three.getData()));
        assertEquals(FrameType.CANCEL, three.getType());

        ts.assertNotTerminated();
        ts.assertNoValues();
    }

    // TODO REQUEST_N on initial frame not implemented yet
	@Test(timeout=2000)
    public void testRequestStreamSuccess() throws InterruptedException {
        TestConnection conn = establishConnection();
        ReplaySubject<Frame> requests = captureRequests(conn);
        LatchedCompletable rc = new LatchedCompletable(1);
        Requester p = Requester.createClientRequester(conn, ConnectionSetupPayload.create("UTF-8", "UTF-8", NO_FLAGS), ERROR_HANDLER, rc);
        rc.await();

        TestSubscriber<String> ts = new TestSubscriber<>();
        fromPublisher(p.requestStream(utf8EncodedPayload("hello", null))).map(pl -> byteToString(pl.getData())).subscribe(ts);

        assertEquals(2, requests.getValues().length);
        List<Frame> requested = requests.take(2).toList().toBlocking().single();

        Frame one = requested.get(0);
        assertEquals(0, one.getStreamId());// SETUP always happens on 0
        assertEquals("", byteToString(one.getData()));
        assertEquals(FrameType.SETUP, one.getType());
        
        Frame two = requested.get(1);
        assertEquals(2, two.getStreamId());// need to start at 2, not 0
        assertEquals("hello", byteToString(two.getData()));
        assertEquals(FrameType.REQUEST_STREAM, two.getType());
        // TODO assert initial requestN
        
        // emit data
        conn.toInput.send(utf8EncodedResponseFrame(2, FrameType.NEXT, "hello"));
        conn.toInput.send(utf8EncodedResponseFrame(2, FrameType.NEXT, "world"));
        conn.toInput.send(utf8EncodedResponseFrame(2, FrameType.COMPLETE, ""));

        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
        ts.assertComplete();
        ts.assertValueSequence(Arrays.asList("hello", "world"));
    }

 // TODO REQUEST_N on initial frame not implemented yet
	@Test(timeout=2000)
    public void testRequestStreamSuccessTake2AndCancel() throws InterruptedException {
        TestConnection conn = establishConnection();
        ReplaySubject<Frame> requests = captureRequests(conn);
        LatchedCompletable rc = new LatchedCompletable(1);
        Requester p = Requester.createClientRequester(conn, ConnectionSetupPayload.create("UTF-8", "UTF-8", NO_FLAGS), ERROR_HANDLER, rc);
        rc.await();

        TestSubscriber<String> ts = new TestSubscriber<>();
        Observable.fromPublisher(p.requestStream(utf8EncodedPayload("hello", null))).take(2).map(pl -> byteToString(pl.getData())).subscribe(ts);

        assertEquals(2, requests.getValues().length);
        List<Frame> requested = requests.take(2).toList().toBlocking().single();

        Frame one = requested.get(0);
        assertEquals(0, one.getStreamId());// SETUP always happens on 0
        assertEquals("", byteToString(one.getData()));
        assertEquals(FrameType.SETUP, one.getType());
        
        Frame two = requested.get(1);
        assertEquals(2, two.getStreamId());// need to start at 2, not 0
        assertEquals("hello", byteToString(two.getData()));
        assertEquals(FrameType.REQUEST_STREAM, two.getType());
        // TODO assert initial requestN

        // emit data
        conn.toInput.send(utf8EncodedResponseFrame(2, FrameType.NEXT, "hello"));
        conn.toInput.send(utf8EncodedResponseFrame(2, FrameType.NEXT, "world"));

        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
        ts.assertComplete();
        ts.assertValueSequence(Arrays.asList("hello", "world"));

        assertEquals(3, requests.getValues().length);
        List<Frame> requested2 = requests.take(3).toList().toBlocking().single();

        // we should have sent a CANCEL
        Frame three = requested2.get(2);
        assertEquals(2, three.getStreamId());// still the same stream
        assertEquals("", byteToString(three.getData()));
        assertEquals(FrameType.CANCEL, three.getType());
    }

	@Test(timeout=2000)
    public void testRequestStreamError() throws InterruptedException {
        TestConnection conn = establishConnection();
        ReplaySubject<Frame> requests = captureRequests(conn);
        LatchedCompletable rc = new LatchedCompletable(1);
        Requester p = Requester.createClientRequester(conn, ConnectionSetupPayload.create("UTF-8", "UTF-8", NO_FLAGS), ERROR_HANDLER, rc);
        rc.await();

        TestSubscriber<Payload> ts = new TestSubscriber<>();
        p.requestStream(utf8EncodedPayload("hello", null)).subscribe(ts);

        assertEquals(2, requests.getValues().length);
        List<Frame> requested = requests.take(2).toList().toBlocking().single();

        Frame one = requested.get(0);
        assertEquals(0, one.getStreamId());// SETUP always happens on 0
        assertEquals("", byteToString(one.getData()));
        assertEquals(FrameType.SETUP, one.getType());
        
        Frame two = requested.get(1);
        assertEquals(2, two.getStreamId());// need to start at 2, not 0
        assertEquals("hello", byteToString(two.getData()));
        assertEquals(FrameType.REQUEST_STREAM, two.getType());
        // TODO assert initial requestN

        // emit data
        conn.toInput.send(utf8EncodedResponseFrame(2, FrameType.NEXT, "hello"));
        conn.toInput.send(utf8EncodedErrorFrame(2, "Failure"));

        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
        ts.assertError(InvalidRequestException.class);
        ts.assertValue(utf8EncodedPayload("hello", null));
        assertEquals("Failure", ts.errors().get(0).getMessage());
    }

    // @Test // TODO need to implement test for REQUEST_N behavior as a long stream is consumed
    public void testRequestStreamRequestNReplenishing() {
        // this should REQUEST(1024), receive 768, REQUEST(768), receive ... etc in a back-and-forth
    }

    /* **********************************************************************************************/

    private static <T> void testCancelBeforeRequestN(Publisher<T> source) {
        TestSubscriber<T> testSubscriber = new CancelBeforeRequestNSubscriber<>();
        source.subscribe(testSubscriber);

        testSubscriber.assertNoErrors();
        testSubscriber.assertNotComplete();
    }

    private static <T> void testRequestLongMaxValue(Publisher<T> source, TestConnection testConnection) {
        List<Integer> requestNs = new ArrayList<>();
        testConnection.write.add(frame -> {
            if (frame.getType() == FrameType.REQUEST_N) {
                requestNs.add(Frame.RequestN.requestN(frame));
            }
        });

        TestSubscriber<T> testSubscriber = new TestSubscriber<T>(1L);
        source.subscribe(testSubscriber);

        testSubscriber.request(Long.MAX_VALUE);
        testSubscriber.assertNoErrors();
        testSubscriber.assertNotComplete();

        MatcherAssert.assertThat("Negative requestNs received.", requestNs, not(contains(-1)));
    }

    private static Requester createClientRequester(TestConnection connection) throws InterruptedException {
        LatchedCompletable rc = new LatchedCompletable(1);
        Requester p = Requester.createClientRequester(connection, ConnectionSetupPayload.create("UTF-8", "UTF-8", NO_FLAGS), ERROR_HANDLER, rc);
        rc.await();
        return p;
    }

    private static Requester createClientRequester() throws InterruptedException {
        return createClientRequester(establishConnection());
    }

	private static TestConnection establishConnection() {
		return new TestConnection();
	}

    private static ReplaySubject<Frame> captureRequests(TestConnection conn) {
        ReplaySubject<Frame> rs = ReplaySubject.create();
        rs.forEach(i -> System.out.println("capturedRequest => " + i));
        conn.write.add(rs::onNext);
        return rs;
    }

    private static class CancelBeforeRequestNSubscriber<T> extends TestSubscriber<T> {
        @Override
        public void onSubscribe(Subscription s) {
            s.cancel();
        }
    }
}
