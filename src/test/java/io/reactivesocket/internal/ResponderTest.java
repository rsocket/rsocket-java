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
import static io.reactivex.Observable.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.junit.Test;
import org.reactivestreams.Subscription;

import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;
import io.reactivesocket.Payload;
import io.reactivesocket.RequestHandler;
import io.reactivesocket.TestConnection;
import io.reactivex.Observable;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.schedulers.TestScheduler;

public class ResponderTest
{
	final static Consumer<Throwable> ERROR_HANDLER = err -> err.printStackTrace();	
	
    @Test
    public void testRequestResponseSuccess() {
    	TestConnection conn = establishConnection();
        Responder.create(conn, setup -> RequestHandler.create(
            request -> just(utf8EncodedPayload(byteToString(request.getData()) + " world", null)),
            null, null, null), ERROR_HANDLER);

        
        Collection<Frame> cachedResponses = captureResponses(conn);
        sendSetupFrame(conn);
        
        // perform a request/response
        conn.toInput.onNext(utf8EncodedRequestFrame(1, FrameType.REQUEST_RESPONSE, "hello", 128));

        assertEquals(1, cachedResponses.size());// 1 onNext + 1 onCompleted
        List<Frame> frames = new ArrayList<>(cachedResponses);

        // assert
        Frame first = frames.get(0);
        assertEquals(1, first.getStreamId());
        assertEquals(FrameType.NEXT_COMPLETE, first.getType());
        assertEquals("hello world", byteToString(first.getData()));
    }

    @Test
    public void testRequestResponseError() {
    	TestConnection conn = establishConnection();
        Responder.create(conn, setup -> RequestHandler.create(
            request -> Observable.<Payload>error(new Exception("Request Not Found")),
            null, null, null), ERROR_HANDLER);

        Collection<Frame> cachedResponses = captureResponses(conn);
        sendSetupFrame(conn);

        // perform a request/response
        conn.toInput.onNext(utf8EncodedRequestFrame(1, FrameType.REQUEST_RESPONSE, "hello", 128));

        // assert
        Frame first = new ArrayList<>(cachedResponses).get(0);
        assertEquals(1, first.getStreamId());
        assertEquals(FrameType.ERROR, first.getType());
        assertEquals("Request Not Found", byteToString(first.getData()));
    }

    @Test
    public void testRequestResponseCancel() {
        AtomicBoolean unsubscribed = new AtomicBoolean();
        Observable<Payload> delayed = Observable.<Payload>never().doOnCancel(() -> unsubscribed.set(true));

        TestConnection conn = establishConnection();
        Responder.create(conn, setup -> RequestHandler.create(
            request -> delayed,
            null, null, null), ERROR_HANDLER);

        Collection<Frame> cachedResponses = captureResponses(conn);
        sendSetupFrame(conn);

        // perform a request/response
        conn.toInput.onNext(utf8EncodedRequestFrame(1, FrameType.REQUEST_RESPONSE, "hello", 128));
        // assert no response
        assertEquals(0, cachedResponses.size());
        // unsubscribe
        assertFalse(unsubscribed.get());
        conn.toInput.onNext(Frame.from(1, FrameType.CANCEL));
        assertTrue(unsubscribed.get());
    }

    @Test
    public void testRequestStreamSuccess() {
    	TestConnection conn = establishConnection();
        Responder.create(conn, setup -> RequestHandler.create(
            null,
            request -> range(Integer.parseInt(byteToString(request.getData())), 10).map(i -> utf8EncodedPayload(i + "!", null)),
            null, null), ERROR_HANDLER);

        Collection<Frame> cachedResponses = captureResponses(conn);
        sendSetupFrame(conn);

        // perform a request/response
        conn.toInput.onNext(utf8EncodedRequestFrame(1, FrameType.REQUEST_STREAM, "10", 128));

        // assert
        assertEquals(11, cachedResponses.size());// 10 onNext + 1 onCompleted
        List<Frame> frames = new ArrayList<>(cachedResponses);

        // 10 onNext frames
        for (int i = 0; i < 10; i++) {
            assertEquals(1, frames.get(i).getStreamId());
            assertEquals(FrameType.NEXT, frames.get(i).getType());
            assertEquals((i + 10) + "!", byteToString(frames.get(i).getData()));
        }

        // last message is a COMPLETE
        assertEquals(1, frames.get(10).getStreamId());
        assertEquals(FrameType.COMPLETE, frames.get(10).getType());
        assertEquals("", byteToString(frames.get(10).getData()));
    }

    @Test
    public void testRequestStreamError() {
    	TestConnection conn = establishConnection();
        Responder.create(conn, setup -> RequestHandler.create(
            null,
            request -> range(Integer.parseInt(byteToString(request.getData())), 3)
                .map(i -> utf8EncodedPayload(i + "!", null))
                .concatWith(error(new Exception("Error Occurred!"))),
            null, null), ERROR_HANDLER);

        Collection<Frame> cachedResponses = captureResponses(conn);
        sendSetupFrame(conn);

        // perform a request/response
        conn.toInput.onNext(utf8EncodedRequestFrame(1, FrameType.REQUEST_STREAM, "0", 128));

        // assert
        assertEquals(4, cachedResponses.size());// 3 onNext + 1 onError
        List<Frame> frames = new ArrayList<>(cachedResponses);

        // 3 onNext frames
        for (int i = 0; i < 3; i++) {
            assertEquals(1, frames.get(i).getStreamId());
            assertEquals(FrameType.NEXT, frames.get(i).getType());
            assertEquals(i + "!", byteToString(frames.get(i).getData()));
        }

        // last message is an ERROR
        assertEquals(1, frames.get(3).getStreamId());
        assertEquals(FrameType.ERROR, frames.get(3).getType());
        assertEquals("Error Occurred!", byteToString(frames.get(3).getData()));
    }

    @Test
    public void testRequestStreamCancel() {
    	TestConnection conn = establishConnection();
        TestScheduler ts = Schedulers.test();
        Responder.create(conn, setup -> RequestHandler.create(
            null,
            request -> interval(1000, TimeUnit.MILLISECONDS, ts).map(i -> utf8EncodedPayload(i + "!", null)),
            null, null), ERROR_HANDLER);

        Collection<Frame> cachedResponses = captureResponses(conn);
        sendSetupFrame(conn);

        // perform a request/response
        conn.toInput.onNext(utf8EncodedRequestFrame(1, FrameType.REQUEST_STREAM, "/aRequest", 128));

        // no time has passed, so no values
        assertEquals(0, cachedResponses.size());
        ts.advanceTimeBy(1000, TimeUnit.MILLISECONDS);
        assertEquals(1, cachedResponses.size());
        ts.advanceTimeBy(2000, TimeUnit.MILLISECONDS);
        assertEquals(3, cachedResponses.size());
        // dispose
        conn.toInput.onNext(Frame.from(1, FrameType.CANCEL));
        // still only 1 message
        assertEquals(3, cachedResponses.size());
        // advance again, nothing should happen
        ts.advanceTimeBy(1000, TimeUnit.MILLISECONDS);
        // should still only have 3 message, no ERROR or COMPLETED
        assertEquals(3, cachedResponses.size());

        List<Frame> frames = new ArrayList<>(cachedResponses);

        // 3 onNext frames
        for (int i = 0; i < 3; i++) {
            assertEquals(1, frames.get(i).getStreamId());
            assertEquals(FrameType.NEXT, frames.get(i).getType());
            assertEquals(i + "!", byteToString(frames.get(i).getData()));
        }
    }

    @Test
    public void testMultiplexedStreams() {
        TestScheduler ts = Schedulers.test();
        TestConnection conn = establishConnection();
        Responder.create(conn, setup -> RequestHandler.create(
            null,
            request -> interval(1000, TimeUnit.MILLISECONDS, ts).map(i -> utf8EncodedPayload(i + "_" + byteToString(request.getData()), null)),
            null, null), ERROR_HANDLER);

        Collection<Frame> cachedResponses = captureResponses(conn);
        sendSetupFrame(conn);

        // perform a request/response
        conn.toInput.onNext(utf8EncodedRequestFrame(1, FrameType.REQUEST_STREAM, "requestA", 128));

        // no time has passed, so no values
        assertEquals(0, cachedResponses.size());
        ts.advanceTimeBy(1000, TimeUnit.MILLISECONDS);
        // we should have 1 message from A
        assertEquals(1, cachedResponses.size());
        // now request another stream
        conn.toInput.onNext(utf8EncodedRequestFrame(2, FrameType.REQUEST_STREAM, "requestB", 128));
        // advance some more
        ts.advanceTimeBy(2000, TimeUnit.MILLISECONDS);
        // should have 3 from A and 2 from B
        assertEquals(5, cachedResponses.size());
        // dispose A, but leave B
        conn.toInput.onNext(Frame.from(1, FrameType.CANCEL));
        // still same 5 frames
        assertEquals(5, cachedResponses.size());
        // advance again, should get 2 from B
        ts.advanceTimeBy(2000, TimeUnit.MILLISECONDS);
        assertEquals(7, cachedResponses.size());

        List<Frame> frames = new ArrayList<>(cachedResponses);

        // A frames (positions 0, 1, 3) incrementing 0, 1, 2
        assertEquals(1, frames.get(0).getStreamId());
        assertEquals("0_requestA", byteToString(frames.get(0).getData()));
        assertEquals(1, frames.get(1).getStreamId());
        assertEquals("1_requestA", byteToString(frames.get(1).getData()));
        assertEquals(1, frames.get(3).getStreamId());
        assertEquals("2_requestA", byteToString(frames.get(3).getData()));

        // B frames (positions 2, 4, 5, 6) incrementing 0, 1, 2, 3
        assertEquals(2, frames.get(2).getStreamId());
        assertEquals("0_requestB", byteToString(frames.get(2).getData()));
        assertEquals(2, frames.get(4).getStreamId());
        assertEquals("1_requestB", byteToString(frames.get(4).getData()));
        assertEquals(2, frames.get(5).getStreamId());
        assertEquals("2_requestB", byteToString(frames.get(5).getData()));
        assertEquals(2, frames.get(6).getStreamId());
        assertEquals("3_requestB", byteToString(frames.get(6).getData()));
    }

    /* **********************************************************************************************/

    private List<Frame> captureResponses(TestConnection conn) {
    	List<Frame> requests = Collections.synchronizedList(new ArrayList<>());
        conn.writes.subscribe(requests::add);
        return requests;
    }

    private TestConnection establishConnection() {
        return new TestConnection();
    }

    private org.reactivestreams.Subscriber<Void> PROTOCOL_SUBSCRIBER = new org.reactivestreams.Subscriber<Void>() {

        @Override
        public void onSubscribe(Subscription s) {
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(Void t) {

        }

        @Override
        public void onError(Throwable t) {
            t.printStackTrace();
        }

        @Override
        public void onComplete() {

        }

    };


	private void sendSetupFrame(TestConnection conn) {
		// setup
        conn.toInput.onNext(Frame.fromSetup(0, 0, 0, "UTF-8", "UTF-8", utf8EncodedPayload("", "")));
	}
}
