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
import static rx.RxReactiveStreams.toObservable;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import rx.Subscription;
import rx.observers.TestSubscriber;
import rx.subjects.ReplaySubject;

public class ReactiveSocketClientProtocolTest {

    @Test
    public void testRequestResponseSuccess() {
        TestConnection conn = establishConnection();
        ReactiveSocketClientProtocol p = ReactiveSocketClientProtocol.create(conn);
        ReplaySubject<Frame> requests = captureRequests(conn);

        TestSubscriber<String> ts = TestSubscriber.create();
        toObservable(p.requestResponse("hello")).subscribe(ts);

        assertEquals(1, requests.getValues().length);
        List<Frame> requested = requests.take(1).toList().toBlocking().single();

        Frame one = requested.get(0);
        assertEquals(1, one.getStreamId());// need to start at 1, not 0
        assertEquals("hello", one.getData());
        assertEquals(FrameType.REQUEST_RESPONSE, one.getType());

        // now emit a response to ensure the Publisher receives and completes
        conn.toInput.onNext(Frame.from(1, FrameType.NEXT, "world"));
        ts.assertValue("world");

        conn.toInput.onNext(Frame.from(1, FrameType.COMPLETE, ""));
        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
        ts.assertCompleted();
    }

    @Test
    public void testRequestResponseError() {
        TestConnection conn = establishConnection();
        ReactiveSocketClientProtocol p = ReactiveSocketClientProtocol.create(conn);
        ReplaySubject<Frame> requests = captureRequests(conn);

        TestSubscriber<String> ts = TestSubscriber.create();
        toObservable(p.requestResponse("hello")).subscribe(ts);

        assertEquals(1, requests.getValues().length);
        List<Frame> requested = requests.take(1).toList().toBlocking().single();

        Frame one = requested.get(0);
        assertEquals(1, one.getStreamId());// need to start at 1, not 0
        assertEquals("hello", one.getData());
        assertEquals(FrameType.REQUEST_RESPONSE, one.getType());

        // now emit a response to ensure the Publisher receives and completes
        conn.toInput.onNext(Frame.from(1, FrameType.NEXT, "world"));
        ts.assertValue("world");

        conn.toInput.onNext(Frame.from(1, FrameType.ERROR, "Failed"));
        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
        ts.assertError(Exception.class);
        assertEquals("Failed", ts.getOnErrorEvents().get(0).getMessage());
    }

    @Test
    public void testRequestResponseCancel() {
        TestConnection conn = establishConnection();
        ReactiveSocketClientProtocol p = ReactiveSocketClientProtocol.create(conn);
        ReplaySubject<Frame> requests = captureRequests(conn);

        TestSubscriber<String> ts = TestSubscriber.create();
        Subscription s = toObservable(p.requestResponse("hello")).subscribe(ts);
        s.unsubscribe();

        assertEquals(2, requests.getValues().length);
        List<Frame> requested = requests.take(2).toList().toBlocking().single();

        Frame one = requested.get(0);
        assertEquals(1, one.getStreamId());// need to start at 1, not 0
        assertEquals("hello", one.getData());
        assertEquals(FrameType.REQUEST_RESPONSE, one.getType());

        Frame two = requested.get(1);
        assertEquals(1, two.getStreamId());// still the same stream
        assertEquals("", two.getData());
        assertEquals(FrameType.CANCEL, two.getType());

        ts.assertNoTerminalEvent();
        ts.assertUnsubscribed();
        ts.assertNoValues();
    }

    @Test
    public void testRequestStreamSuccess() {
        TestConnection conn = establishConnection();
        ReactiveSocketClientProtocol p = ReactiveSocketClientProtocol.create(conn);
        ReplaySubject<Frame> requests = captureRequests(conn);

        TestSubscriber<String> ts = TestSubscriber.create();
        toObservable(p.requestStream("hello")).subscribe(ts);

        assertEquals(2, requests.getValues().length);
        List<Frame> requested = requests.take(2).toList().toBlocking().single();

        Frame one = requested.get(0);
        assertEquals(1, one.getStreamId());// need to start at 1, not 0
        assertEquals("hello", one.getData());
        assertEquals(FrameType.REQUEST_STREAM, one.getType());

        Frame two = requested.get(1);
        assertEquals(1, two.getStreamId());// still the same stream
        assertEquals(String.valueOf(Long.MAX_VALUE), two.getData());// TODO we should alter the default to something like 1024 when MAX_VALUE is requested
        assertEquals(FrameType.REQUEST_N, two.getType());

        // emit data
        conn.toInput.onNext(Frame.from(1, FrameType.NEXT, "hello"));
        conn.toInput.onNext(Frame.from(1, FrameType.NEXT, "world"));
        conn.toInput.onNext(Frame.from(1, FrameType.COMPLETE, ""));

        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
        ts.assertCompleted();
        ts.assertReceivedOnNext(Arrays.asList("hello", "world"));
    }

    @Test
    public void testRequestStreamSuccessTake2AndCancel() {
        TestConnection conn = establishConnection();
        ReactiveSocketClientProtocol p = ReactiveSocketClientProtocol.create(conn);
        ReplaySubject<Frame> requests = captureRequests(conn);

        TestSubscriber<String> ts = TestSubscriber.create();
        toObservable(p.requestStream("hello")).take(2).subscribe(ts);

        assertEquals(2, requests.getValues().length);
        List<Frame> requested = requests.take(2).toList().toBlocking().single();

        Frame one = requested.get(0);
        assertEquals(1, one.getStreamId());// need to start at 1, not 0
        assertEquals("hello", one.getData());
        assertEquals(FrameType.REQUEST_STREAM, one.getType());

        Frame two = requested.get(1);
        assertEquals(1, two.getStreamId());// still the same stream
        assertEquals(String.valueOf(2), two.getData());
        assertEquals(FrameType.REQUEST_N, two.getType());

        // emit data
        conn.toInput.onNext(Frame.from(1, FrameType.NEXT, "hello"));
        conn.toInput.onNext(Frame.from(1, FrameType.NEXT, "world"));

        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
        ts.assertCompleted();
        ts.assertReceivedOnNext(Arrays.asList("hello", "world"));

        assertEquals(3, requests.getValues().length);
        List<Frame> requested2 = requests.take(3).toList().toBlocking().single();

        // we should have sent a CANCEL
        Frame three = requested2.get(2);
        assertEquals(1, three.getStreamId());// still the same stream
        assertEquals("", three.getData());
        assertEquals(FrameType.CANCEL, three.getType());
    }

    @Test
    public void testRequestStreamError() {
        TestConnection conn = establishConnection();
        ReactiveSocketClientProtocol p = ReactiveSocketClientProtocol.create(conn);
        ReplaySubject<Frame> requests = captureRequests(conn);

        TestSubscriber<String> ts = TestSubscriber.create();
        toObservable(p.requestStream("hello")).subscribe(ts);

        assertEquals(2, requests.getValues().length);
        List<Frame> requested = requests.take(2).toList().toBlocking().single();

        Frame one = requested.get(0);
        assertEquals(1, one.getStreamId());// need to start at 1, not 0
        assertEquals("hello", one.getData());
        assertEquals(FrameType.REQUEST_STREAM, one.getType());

        Frame two = requested.get(1);
        assertEquals(1, two.getStreamId());// still the same stream
        assertEquals(String.valueOf(Long.MAX_VALUE), two.getData());// TODO we should alter the default to something like 1024 when MAX_VALUE is requested
        assertEquals(FrameType.REQUEST_N, two.getType());

        // emit data
        conn.toInput.onNext(Frame.from(1, FrameType.NEXT, "hello"));
        conn.toInput.onNext(Frame.from(1, FrameType.ERROR, "Failure"));

        ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
        ts.assertError(Exception.class);
        ts.assertReceivedOnNext(Arrays.asList("hello"));
        assertEquals("Failure", ts.getOnErrorEvents().get(0).getMessage());
    }

    // @Test // TODO need to implement test for REQUEST_N behavior as a long stream is consumed
    public void testRequestStreamRequestNReplenishing() {
        // this should REQUEST(1024), receive 768, REQUEST(768), receive ... etc in a back-and-forth
    }

    /* **********************************************************************************************/

    private TestConnection establishConnection() {
        TestConnection conn = new TestConnection();
        return conn;
    }

    private ReplaySubject<Frame> captureRequests(TestConnection conn) {
        ReplaySubject<Frame> rs = ReplaySubject.create();
        rs.forEach(i -> System.out.println("capturedRequest => " + i));
        conn.writes.subscribe(rs);
        return rs;
    }

}
