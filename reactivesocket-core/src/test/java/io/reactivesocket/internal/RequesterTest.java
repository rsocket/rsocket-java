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

import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;
import io.reactivesocket.LatchedCompletable;
import io.reactivesocket.Payload;
import io.reactivesocket.TestConnection;
import org.junit.Test;
import rx.observers.TestSubscriber;
import rx.subjects.ReplaySubject;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.reactivesocket.ConnectionSetupPayload.*;
import static io.reactivesocket.TestUtil.*;
import static org.junit.Assert.*;
import static rx.RxReactiveStreams.*;

public class RequesterTest
{
	final static Consumer<Throwable> ERROR_HANDLER = Throwable::printStackTrace;

	@Test(timeout=2000)
    public void testRequestResponseSuccess() throws InterruptedException {
        TestConnection conn = establishConnection();
        ReplaySubject<Frame> requests = captureRequests(conn);
        LatchedCompletable rc = new LatchedCompletable(1);
        Requester p = Requester.createClientRequester(conn, create("UTF-8", "UTF-8", NO_FLAGS), ERROR_HANDLER, rc);
        rc.await();

        TestSubscriber<Payload> ts = testSubscribe(p.requestResponse(utf8EncodedPayload("hello", null)));

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
        ts.assertCompleted();
    }

	@Test(timeout=2000)
    public void testRequestResponseError() throws InterruptedException {
        TestConnection conn = establishConnection();
        ReplaySubject<Frame> requests = captureRequests(conn);
        LatchedCompletable rc = new LatchedCompletable(1);
        Requester p = Requester.createClientRequester(conn, create("UTF-8", "UTF-8", NO_FLAGS), ERROR_HANDLER, rc);
        rc.await();

        TestSubscriber<Payload> ts = testSubscribe(p.requestResponse(utf8EncodedPayload("hello", null)));

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
        ts.assertError(Exception.class);
        assertEquals("Failed", ts.getOnErrorEvents().get(0).getMessage());
    }

	@Test(timeout=2000)
    public void testRequestResponseCancel() throws InterruptedException {
        TestConnection conn = establishConnection();
        ReplaySubject<Frame> requests = captureRequests(conn);
        LatchedCompletable rc = new LatchedCompletable(1);
        Requester p = Requester.createClientRequester(conn, create("UTF-8", "UTF-8", NO_FLAGS), ERROR_HANDLER, rc);
        rc.await();

        TestSubscriber<Payload> ts = testSubscribe(p.requestResponse(utf8EncodedPayload("hello", null)));
        ts.unsubscribe();

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

        ts.assertNoTerminalEvent();
        ts.assertNoValues();
    }

    // TODO REQUEST_N on initial frame not implemented yet
	@Test(timeout=2000)
    public void testRequestStreamSuccess() throws InterruptedException {
        TestConnection conn = establishConnection();
        ReplaySubject<Frame> requests = captureRequests(conn);
        LatchedCompletable rc = new LatchedCompletable(1);
        Requester p = Requester.createClientRequester(conn, create("UTF-8", "UTF-8", NO_FLAGS), ERROR_HANDLER, rc);
        rc.await();

        TestSubscriber<String> ts = new TestSubscriber<>();
        toObservable(p.requestStream(utf8EncodedPayload("hello", null))).map(pl -> byteToString(pl.getData()))
                                                                        .subscribe(ts);

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
        ts.assertCompleted();
        ts.assertValues("hello", "world");
    }

 // TODO REQUEST_N on initial frame not implemented yet
	@Test(timeout=2000)
    public void testRequestStreamSuccessTake2AndCancel() throws InterruptedException {
        TestConnection conn = establishConnection();
        ReplaySubject<Frame> requests = captureRequests(conn);
        LatchedCompletable rc = new LatchedCompletable(1);
        Requester p = Requester.createClientRequester(conn, create("UTF-8", "UTF-8", NO_FLAGS), ERROR_HANDLER, rc);
        rc.await();

        TestSubscriber<String> ts = new TestSubscriber<>();
        toObservable(p.requestStream(utf8EncodedPayload("hello", null))).take(2).map(pl -> byteToString(pl.getData()))
                                                                        .subscribe(ts);

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
        ts.assertCompleted();
        ts.assertValues("hello", "world");

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
        Requester p = Requester.createClientRequester(conn, create("UTF-8", "UTF-8", NO_FLAGS), ERROR_HANDLER, rc);
        rc.await();

        TestSubscriber<Payload> ts = testSubscribe(p.requestStream(utf8EncodedPayload("hello", null)));

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
        ts.assertError(Exception.class);
        ts.assertValue(utf8EncodedPayload("hello", null));
        assertEquals("Failure", ts.getOnErrorEvents().get(0).getMessage());
    }

    // @Test // TODO need to implement test for REQUEST_N behavior as a long stream is consumed
    public void testRequestStreamRequestNReplenishing() {
        // this should REQUEST(1024), receive 768, REQUEST(768), receive ... etc in a back-and-forth
    }

    /* **********************************************************************************************/

	private static TestConnection establishConnection() {
		return new TestConnection();
	}

    private static ReplaySubject<Frame> captureRequests(TestConnection conn) {
        ReplaySubject<Frame> rs = ReplaySubject.create();
        rs.forEach(i -> System.out.println("capturedRequest => " + i));
        conn.write.add(rs::onNext);
        return rs;
    }
}
