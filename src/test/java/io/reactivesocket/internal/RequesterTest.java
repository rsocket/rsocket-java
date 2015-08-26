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

import static io.reactivesocket.TestUtil.*;

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;
import io.reactivesocket.Payload;
import io.reactivesocket.TestConnection;
import io.reactivesocket.internal.Requester;
import rx.observers.TestSubscriber;
import rx.subjects.ReplaySubject;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static rx.RxReactiveStreams.toObservable;

public class RequesterTest
{
    @Test
    public void testRequestResponseSuccess() {
        TestConnection conn = establishConnection();
        Requester p = Requester.createClientRequester(conn, ConnectionSetupPayload.create("UTF-8", "UTF-8"));
        toObservable(p.start()).subscribe();
        ReplaySubject<Frame> requests = captureRequests(conn);

        TestSubscriber<Payload> ts = TestSubscriber.create();
        toObservable(p.requestResponse(utf8EncodedPayload("hello", null))).subscribe(ts);

        ts.assertNoErrors();
        assertEquals(1, requests.getValues().length);
        List<Frame> requested = requests.take(1).toList().toBlocking().single();

        Frame one = requested.get(0);
        assertEquals(2, one.getStreamId());// need to start at 2, not 0
        assertEquals("hello", byteToString(one.getData()));
        assertEquals(FrameType.REQUEST_RESPONSE, one.getType());
        
        // now emit a response to ensure the Publisher receives and completes
        conn.toInput.onNext(utf8EncodedFrame(2, FrameType.NEXT_COMPLETE, "world"));

        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
        ts.assertValue(utf8EncodedPayload("world", null));
        ts.assertCompleted();
    }

    @Test
    public void testRequestResponseError() {
        TestConnection conn = establishConnection();
        Requester p = Requester.createClientRequester(conn, ConnectionSetupPayload.create("UTF-8", "UTF-8"));
        toObservable(p.start()).subscribe();
        ReplaySubject<Frame> requests = captureRequests(conn);

        TestSubscriber<Payload> ts = TestSubscriber.create();
        toObservable(p.requestResponse(utf8EncodedPayload("hello", null))).subscribe(ts);

        assertEquals(1, requests.getValues().length);
        List<Frame> requested = requests.take(1).toList().toBlocking().single();

        Frame one = requested.get(0);
        assertEquals(2, one.getStreamId());// need to start at 2, not 0
        assertEquals("hello", byteToString(one.getData()));
        assertEquals(FrameType.REQUEST_RESPONSE, one.getType());

        conn.toInput.onNext(Frame.fromError(2, new RuntimeException("Failed")));
        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
        ts.assertError(Exception.class);
        assertEquals("Failed", ts.getOnErrorEvents().get(0).getMessage());
    }

    @Test
    public void testRequestResponseCancel() {
        TestConnection conn = establishConnection();
        Requester p = Requester.createClientRequester(conn, ConnectionSetupPayload.create("UTF-8", "UTF-8"));
        toObservable(p.start()).subscribe();
        ReplaySubject<Frame> requests = captureRequests(conn);

        TestSubscriber<Payload> ts = TestSubscriber.create();
        rx.Subscription s = toObservable(p.requestResponse(utf8EncodedPayload("hello", null))).subscribe(ts);
        s.unsubscribe();

        assertEquals(2, requests.getValues().length);
        List<Frame> requested = requests.take(2).toList().toBlocking().single();

        Frame one = requested.get(0);
        assertEquals(2, one.getStreamId());// need to start at 2, not 0
        assertEquals("hello", byteToString(one.getData()));
        assertEquals(FrameType.REQUEST_RESPONSE, one.getType());

        Frame two = requested.get(1);
        assertEquals(2, two.getStreamId());// still the same stream
        assertEquals("", byteToString(two.getData()));
        assertEquals(FrameType.CANCEL, two.getType());

        ts.assertNoTerminalEvent();
        ts.assertUnsubscribed();
        ts.assertNoValues();
    }

    // TODO REQUEST_N on initial frame not implemented yet
    @Test
    public void testRequestStreamSuccess() {
        TestConnection conn = establishConnection();
        Requester p = Requester.createClientRequester(conn, ConnectionSetupPayload.create("UTF-8", "UTF-8"));
        toObservable(p.start()).subscribe();
        ReplaySubject<Frame> requests = captureRequests(conn);

        TestSubscriber<Payload> ts = TestSubscriber.create();
        toObservable(p.requestStream(utf8EncodedPayload("hello", null))).subscribe(ts);

        assertEquals(1, requests.getValues().length);
        List<Frame> requested = requests.take(1).toList().toBlocking().single();

        Frame one = requested.get(0);
        assertEquals(2, one.getStreamId());// need to start at 2, not 0
        assertEquals("hello", byteToString(one.getData()));
        assertEquals(FrameType.REQUEST_STREAM, one.getType());
        // TODO assert initial requestN
        
        // emit data
        conn.toInput.onNext(utf8EncodedFrame(2, FrameType.NEXT, "hello"));
        conn.toInput.onNext(utf8EncodedFrame(2, FrameType.NEXT, "world"));
        conn.toInput.onNext(utf8EncodedFrame(2, FrameType.COMPLETE, ""));

        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
        ts.assertCompleted();
        ts.assertReceivedOnNext(Arrays.asList(utf8EncodedPayload("hello", null), utf8EncodedPayload("world", null)));
    }

 // TODO REQUEST_N on initial frame not implemented yet
    @Test
    public void testRequestStreamSuccessTake2AndCancel() {
        TestConnection conn = establishConnection();
        Requester p = Requester.createClientRequester(conn, ConnectionSetupPayload.create("UTF-8", "UTF-8"));
        toObservable(p.start()).subscribe();
        ReplaySubject<Frame> requests = captureRequests(conn);

        TestSubscriber<Payload> ts = TestSubscriber.create();
        toObservable(p.requestStream(utf8EncodedPayload("hello", null))).take(2).subscribe(ts);

        assertEquals(1, requests.getValues().length);
        List<Frame> requested = requests.take(1).toList().toBlocking().single();

        Frame one = requested.get(0);
        assertEquals(2, one.getStreamId());// need to start at 2, not 0
        assertEquals("hello", byteToString(one.getData()));
        assertEquals(FrameType.REQUEST_STREAM, one.getType());
        // TODO assert initial requestN

        // emit data
        conn.toInput.onNext(utf8EncodedFrame(2, FrameType.NEXT, "hello"));
        conn.toInput.onNext(utf8EncodedFrame(2, FrameType.NEXT, "world"));

        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
        ts.assertCompleted();
        ts.assertReceivedOnNext(Arrays.asList(utf8EncodedPayload("hello", null), utf8EncodedPayload("world", null)));

        assertEquals(2, requests.getValues().length);
        List<Frame> requested2 = requests.take(2).toList().toBlocking().single();

        // we should have sent a CANCEL
        Frame two = requested2.get(1);
        assertEquals(2, two.getStreamId());// still the same stream
        assertEquals("", byteToString(two.getData()));
        assertEquals(FrameType.CANCEL, two.getType());
    }

    @Test
    public void testRequestStreamError() {
        TestConnection conn = establishConnection();
        Requester p = Requester.createClientRequester(conn, ConnectionSetupPayload.create("UTF-8", "UTF-8"));
        toObservable(p.start()).subscribe();
        ReplaySubject<Frame> requests = captureRequests(conn);

        TestSubscriber<Payload> ts = TestSubscriber.create();
        toObservable(p.requestStream(utf8EncodedPayload("hello", null))).subscribe(ts);

        assertEquals(1, requests.getValues().length);
        List<Frame> requested = requests.take(1).toList().toBlocking().single();

        Frame one = requested.get(0);
        assertEquals(2, one.getStreamId());// need to start at 2, not 0
        assertEquals("hello", byteToString(one.getData()));
        assertEquals(FrameType.REQUEST_STREAM, one.getType());
        // TODO assert initial requestN

        // emit data
        conn.toInput.onNext(utf8EncodedFrame(2, FrameType.NEXT, "hello"));
        conn.toInput.onNext(utf8EncodedFrame(2, FrameType.ERROR, "Failure"));

        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
        ts.assertError(Exception.class);
        ts.assertReceivedOnNext(Arrays.asList(utf8EncodedPayload("hello", null)));
        assertEquals("Failure", ts.getOnErrorEvents().get(0).getMessage());
    }

    // @Test // TODO need to implement test for REQUEST_N behavior as a long stream is consumed
    public void testRequestStreamRequestNReplenishing() {
        // this should REQUEST(1024), receive 768, REQUEST(768), receive ... etc in a back-and-forth
    }

    /* **********************************************************************************************/

	private TestConnection establishConnection() {
		return new TestConnection();
	}

    private ReplaySubject<Frame> captureRequests(TestConnection conn) {
        ReplaySubject<Frame> rs = ReplaySubject.create();
        rs.forEach(i -> System.out.println("capturedRequest => " + i));
        conn.writes.subscribe(rs);
        return rs;
    }

}
