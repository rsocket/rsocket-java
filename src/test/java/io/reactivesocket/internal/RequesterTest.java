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

import static io.reactivesocket.ConnectionSetupPayload.*;
import static io.reactivesocket.TestUtil.*;
import static io.reactivex.Observable.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.Test;

import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;
import io.reactivesocket.Payload;
import io.reactivesocket.TestConnection;
import io.reactivex.subscribers.TestSubscriber;

public class RequesterTest
{
	final static Consumer<Throwable> ERROR_HANDLER = err -> err.printStackTrace();
	
    @Test
    public void testRequestResponseSuccess() throws InterruptedException {
        TestConnection conn = establishConnection();
        Requester p = Requester.createClientRequester(conn, ConnectionSetupPayload.create("UTF-8", "UTF-8", NO_FLAGS), ERROR_HANDLER);
        Set<Frame> requests = captureRequests(conn);

        TestSubscriber<Payload> ts = new TestSubscriber<>();
        fromPublisher(p.requestResponse(utf8EncodedPayload("hello", null))).subscribe(ts);

        ts.assertNoError();
        assertEquals(1, requests.size());
        List<Frame> requested = new ArrayList<>(requests);
        
        Frame one = requested.get(0);
        assertEquals(2, one.getStreamId());// need to start at 2, not 0
        assertEquals("hello", byteToString(one.getData()));
        assertEquals(FrameType.REQUEST_RESPONSE, one.getType());
        
        // now emit a response to ensure the Publisher receives and completes
        conn.toInput.onNext(utf8EncodedFrame(2, FrameType.NEXT_COMPLETE, "world"));

        ts.await(1000, TimeUnit.MILLISECONDS);
        ts.assertValue(utf8EncodedPayload("world", null));
        ts.assertComplete();
    }

    @Test
    public void testRequestResponseError() throws InterruptedException {
        TestConnection conn = establishConnection();
        Requester p = Requester.createClientRequester(conn, ConnectionSetupPayload.create("UTF-8", "UTF-8", NO_FLAGS), ERROR_HANDLER);
        Set<Frame> requests = captureRequests(conn);

        TestSubscriber<Payload> ts = new TestSubscriber<>();
        fromPublisher(p.requestResponse(utf8EncodedPayload("hello", null))).subscribe(ts);

        assertEquals(1, requests.size());
        List<Frame> requested = new ArrayList<>(requests);

        Frame one = requested.get(0);
        assertEquals(2, one.getStreamId());// need to start at 2, not 0
        assertEquals("hello", byteToString(one.getData()));
        assertEquals(FrameType.REQUEST_RESPONSE, one.getType());

        conn.toInput.onNext(Frame.fromError(2, new RuntimeException("Failed")));
        ts.await(1000, TimeUnit.MILLISECONDS);
        ts.assertError(Exception.class);
        assertEquals("Failed", ts.errors().get(0).getMessage());
    }

    @Test
    public void testRequestResponseCancel() {
        TestConnection conn = establishConnection();
        Requester p = Requester.createClientRequester(conn, ConnectionSetupPayload.create("UTF-8", "UTF-8", NO_FLAGS), ERROR_HANDLER);
        Set<Frame> requests = captureRequests(conn);

        TestSubscriber<Payload> ts = new TestSubscriber<>();
        fromPublisher(p.requestResponse(utf8EncodedPayload("hello", null))).subscribe(ts);
        ts.cancel();

        assertEquals(2, requests.size());
        List<Frame> requested = new ArrayList<>(requests);

        Frame one = requested.get(0);
        assertEquals(2, one.getStreamId());// need to start at 2, not 0
        assertEquals("hello", byteToString(one.getData()));
        assertEquals(FrameType.REQUEST_RESPONSE, one.getType());

        Frame two = requested.get(1);
        assertEquals(2, two.getStreamId());// still the same stream
        assertEquals("", byteToString(two.getData()));
        assertEquals(FrameType.CANCEL, two.getType());

        ts.assertNotTerminated();
        ts.assertNoValues();
    }

    // TODO REQUEST_N on initial frame not implemented yet
    @Test
    public void testRequestStreamSuccess() throws InterruptedException {
        TestConnection conn = establishConnection();
        Requester p = Requester.createClientRequester(conn, ConnectionSetupPayload.create("UTF-8", "UTF-8", NO_FLAGS), ERROR_HANDLER);
        Set<Frame> requests = captureRequests(conn);

        TestSubscriber<Payload> ts = new TestSubscriber<>();
        fromPublisher(p.requestStream(utf8EncodedPayload("hello", null))).subscribe(ts);

        assertEquals(1, requests.size());
        List<Frame> requested = new ArrayList<>(requests);

        Frame one = requested.get(0);
        assertEquals(2, one.getStreamId());// need to start at 2, not 0
        assertEquals("hello", byteToString(one.getData()));
        assertEquals(FrameType.REQUEST_STREAM, one.getType());
        // TODO assert initial requestN
        
        // emit data
        conn.toInput.onNext(utf8EncodedFrame(2, FrameType.NEXT, "hello"));
        conn.toInput.onNext(utf8EncodedFrame(2, FrameType.NEXT, "world"));
        conn.toInput.onNext(utf8EncodedFrame(2, FrameType.COMPLETE, ""));

        ts.await(1000, TimeUnit.MILLISECONDS);
        ts.assertComplete();
        ts.assertValueSequence(Arrays.asList(utf8EncodedPayload("hello", null), utf8EncodedPayload("world", null)));
    }

 // TODO REQUEST_N on initial frame not implemented yet
    @Test
    public void testRequestStreamSuccessTake2AndCancel() throws InterruptedException {
        TestConnection conn = establishConnection();
        Requester p = Requester.createClientRequester(conn, ConnectionSetupPayload.create("UTF-8", "UTF-8", NO_FLAGS), ERROR_HANDLER);
        Set<Frame> requests = captureRequests(conn);

        TestSubscriber<Payload> ts = new TestSubscriber<>();
        fromPublisher(p.requestStream(utf8EncodedPayload("hello", null))).take(2).subscribe(ts);

        assertEquals(1, requests.size());
        List<Frame> requested = new ArrayList<>(requests);

        Frame one = requested.get(0);
        assertEquals(2, one.getStreamId());// need to start at 2, not 0
        assertEquals("hello", byteToString(one.getData()));
        assertEquals(FrameType.REQUEST_STREAM, one.getType());
        // TODO assert initial requestN

        // emit data
        conn.toInput.onNext(utf8EncodedFrame(2, FrameType.NEXT, "hello"));
        conn.toInput.onNext(utf8EncodedFrame(2, FrameType.NEXT, "world"));

        ts.await(1000, TimeUnit.MILLISECONDS);
        ts.assertComplete();
        ts.assertValueSequence(Arrays.asList(utf8EncodedPayload("hello", null), utf8EncodedPayload("world", null)));

        assertEquals(2, requests.size());
        List<Frame> requested2 = new ArrayList<>(requests);

        // we should have sent a CANCEL
        Frame two = requested2.get(1);
        assertEquals(2, two.getStreamId());// still the same stream
        assertEquals("", byteToString(two.getData()));
        assertEquals(FrameType.CANCEL, two.getType());
    }

    @Test
    public void testRequestStreamError() throws InterruptedException {
        TestConnection conn = establishConnection();
        Requester p = Requester.createClientRequester(conn, ConnectionSetupPayload.create("UTF-8", "UTF-8", NO_FLAGS), ERROR_HANDLER);
        Set<Frame> requests = captureRequests(conn);

        TestSubscriber<Payload> ts = new TestSubscriber<>();
        fromPublisher(p.requestStream(utf8EncodedPayload("hello", null))).subscribe(ts);

        assertEquals(1, requests.size());
        List<Frame> requested = new ArrayList<>(requests);

        Frame one = requested.get(0);
        assertEquals(2, one.getStreamId());// need to start at 2, not 0
        assertEquals("hello", byteToString(one.getData()));
        assertEquals(FrameType.REQUEST_STREAM, one.getType());
        // TODO assert initial requestN

        // emit data
        conn.toInput.onNext(utf8EncodedFrame(2, FrameType.NEXT, "hello"));
        conn.toInput.onNext(utf8EncodedFrame(2, FrameType.ERROR, "Failure"));

        ts.await(1000, TimeUnit.MILLISECONDS);
        ts.assertError(Exception.class);
        ts.assertValueSequence(Arrays.asList(utf8EncodedPayload("hello", null)));
        assertEquals("Failure", ts.errors().get(0).getMessage());
    }

    // @Test // TODO need to implement test for REQUEST_N behavior as a long stream is consumed
    public void testRequestStreamRequestNReplenishing() {
        // this should REQUEST(1024), receive 768, REQUEST(768), receive ... etc in a back-and-forth
    }

    /* **********************************************************************************************/

	private TestConnection establishConnection() {
		return new TestConnection();
	}

    private C<Frame> captureRequests(TestConnection conn) {
    	ConcurrentLinkedQueue<Frame> requests = new ConcurrentLinkedQueue<>(); 
        conn.writes.subscribe(requests::add);
        return requests;
    }

}
