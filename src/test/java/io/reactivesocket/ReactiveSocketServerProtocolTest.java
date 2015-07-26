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

import static org.junit.Assert.*;
import static rx.Observable.*;
import static rx.RxReactiveStreams.*;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.reactivestreams.Subscription;

import rx.Observable;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;
import rx.subjects.ReplaySubject;

public class ReactiveSocketServerProtocolTest {

    @Test
    public void testRequestResponseSuccess() {
        ReactiveSocketServerProtocol p = ReactiveSocketServerProtocol.create(RequestHandler.create(
                request -> toPublisher(just(request + " world")),
                null, null, null));

        TestConnection conn = establishConnection(p);
        ReplaySubject<Message> cachedResponses = captureResponses(conn);

        // perform a request/response
        conn.toInput.onNext(Message.from(1, MessageType.REQUEST_RESPONSE, "hello"));

        // TODO do we want to receive 2 messages, or just a single NEXT_COMPLETE?
        assertEquals(2, cachedResponses.getValues().length);// 1 onNext + 1 onCompleted
        List<Message> messages = cachedResponses.take(2).toList().toBlocking().first();

        // assert
        Message first = messages.get(0);
        assertEquals(1, first.getStreamId());
        assertEquals(MessageType.NEXT, first.getMessageType());
        assertEquals("hello world", first.getMessage());

        Message second = messages.get(1);
        assertEquals(1, second.getStreamId());
        assertEquals(MessageType.COMPLETE, second.getMessageType());
        assertEquals("", second.getMessage());
    }

    @Test
    public void testRequestResponseError() {
        ReactiveSocketServerProtocol p = ReactiveSocketServerProtocol.create(RequestHandler.create(
                request -> toPublisher(Observable.<String> error(new Exception("Request Not Found"))),
                null, null, null));

        TestConnection conn = establishConnection(p);
        Observable<Message> cachedResponses = captureResponses(conn);

        // perform a request/response
        conn.toInput.onNext(Message.from(1, MessageType.REQUEST_RESPONSE, "hello"));

        // assert
        Message first = cachedResponses.toBlocking().first();
        assertEquals(1, first.getStreamId());
        assertEquals(MessageType.ERROR, first.getMessageType());
        assertEquals("Request Not Found", first.getMessage());
    }

    @Test
    public void testRequestResponseCancel() {
        AtomicBoolean unsubscribed = new AtomicBoolean();
        Observable<String> delayed = never()
                .cast(String.class)
                .doOnUnsubscribe(() -> unsubscribed.set(true));

        ReactiveSocketServerProtocol p = ReactiveSocketServerProtocol.create(RequestHandler.create(
                request -> toPublisher(delayed),
                null, null, null));

        TestConnection conn = establishConnection(p);
        ReplaySubject<Message> cachedResponses = captureResponses(conn);

        // perform a request/response
        conn.toInput.onNext(Message.from(1, MessageType.REQUEST_RESPONSE, "hello"));
        // assert no response
        assertFalse(cachedResponses.hasAnyValue());
        // unsubscribe
        assertFalse(unsubscribed.get());
        conn.toInput.onNext(Message.from(1, MessageType.CANCEL, ""));
        assertTrue(unsubscribed.get());
    }

    @Test
    public void testRequestStreamSuccess() {
        ReactiveSocketServerProtocol p = ReactiveSocketServerProtocol.create(RequestHandler.create(
                null,
                request -> toPublisher(range(Integer.parseInt(request), 10).map(i -> i + "!")),
                null, null));

        TestConnection conn = establishConnection(p);
        ReplaySubject<Message> cachedResponses = captureResponses(conn);

        // perform a request/response
        conn.toInput.onNext(Message.from(1, MessageType.REQUEST_STREAM, "10"));

        // assert
        assertEquals(11, cachedResponses.getValues().length);// 10 onNext + 1 onCompleted
        List<Message> messages = cachedResponses.take(11).toList().toBlocking().first();

        // 10 onNext messages
        for (int i = 0; i < 10; i++) {
            assertEquals(1, messages.get(i).getStreamId());
            assertEquals(MessageType.NEXT, messages.get(i).getMessageType());
            assertEquals((i + 10) + "!", messages.get(i).getMessage());
        }

        // last message is a COMPLETE
        assertEquals(1, messages.get(10).getStreamId());
        assertEquals(MessageType.COMPLETE, messages.get(10).getMessageType());
        assertEquals("", messages.get(10).getMessage());
    }

    @Test
    public void testRequestStreamError() {
        ReactiveSocketServerProtocol p = ReactiveSocketServerProtocol.create(RequestHandler.create(
                null,
                request -> toPublisher(range(Integer.parseInt(request), 3)
                        .map(i -> i + "!")
                        .concatWith(error(new Exception("Error Occurred!")))),
                null, null));

        TestConnection conn = establishConnection(p);
        ReplaySubject<Message> cachedResponses = captureResponses(conn);

        // perform a request/response
        conn.toInput.onNext(Message.from(1, MessageType.REQUEST_STREAM, "0"));

        // assert
        assertEquals(4, cachedResponses.getValues().length);// 3 onNext + 1 onError
        List<Message> messages = cachedResponses.take(4).toList().toBlocking().first();

        // 3 onNext messages
        for (int i = 0; i < 3; i++) {
            assertEquals(1, messages.get(i).getStreamId());
            assertEquals(MessageType.NEXT, messages.get(i).getMessageType());
            assertEquals(i + "!", messages.get(i).getMessage());
        }

        // last message is an ERROR
        assertEquals(1, messages.get(3).getStreamId());
        assertEquals(MessageType.ERROR, messages.get(3).getMessageType());
        assertEquals("Error Occurred!", messages.get(3).getMessage());
    }

    @Test
    public void testRequestStreamCancel() {
        TestScheduler ts = Schedulers.test();
        ReactiveSocketServerProtocol p = ReactiveSocketServerProtocol.create(RequestHandler.create(
                null,
                request -> toPublisher(interval(1000, TimeUnit.MILLISECONDS, ts).map(i -> i + "!")),
                null, null));

        TestConnection conn = establishConnection(p);
        ReplaySubject<Message> cachedResponses = captureResponses(conn);

        // perform a request/response
        conn.toInput.onNext(Message.from(1, MessageType.REQUEST_STREAM, "/aRequest"));

        // no time has passed, so no values
        assertEquals(0, cachedResponses.getValues().length);
        ts.advanceTimeBy(1000, TimeUnit.MILLISECONDS);
        assertEquals(1, cachedResponses.getValues().length);
        ts.advanceTimeBy(2000, TimeUnit.MILLISECONDS);
        assertEquals(3, cachedResponses.getValues().length);
        // dispose
        conn.toInput.onNext(Message.from(1, MessageType.CANCEL, ""));
        // still only 1 message
        assertEquals(3, cachedResponses.getValues().length);
        // advance again, nothing should happen
        ts.advanceTimeBy(1000, TimeUnit.MILLISECONDS);
        // should still only have 3 message, no ERROR or COMPLETED
        assertEquals(3, cachedResponses.getValues().length);

        List<Message> messages = cachedResponses.take(3).toList().toBlocking().first();

        // 3 onNext messages
        for (int i = 0; i < 3; i++) {
            assertEquals(1, messages.get(i).getStreamId());
            assertEquals(MessageType.NEXT, messages.get(i).getMessageType());
            assertEquals(i + "!", messages.get(i).getMessage());
        }
    }

    @Test
    public void testMultiplexedStreams() {
        TestScheduler ts = Schedulers.test();
        ReactiveSocketServerProtocol p = ReactiveSocketServerProtocol.create(RequestHandler.create(
                null,
                request -> toPublisher(interval(1000, TimeUnit.MILLISECONDS, ts).map(i -> i + "_" + request)),
                null, null));

        TestConnection conn = establishConnection(p);
        ReplaySubject<Message> cachedResponses = captureResponses(conn);

        // perform a request/response
        conn.toInput.onNext(Message.from(1, MessageType.REQUEST_STREAM, "requestA"));

        // no time has passed, so no values
        assertEquals(0, cachedResponses.getValues().length);
        ts.advanceTimeBy(1000, TimeUnit.MILLISECONDS);
        // we should have 1 message from A
        assertEquals(1, cachedResponses.getValues().length);
        // now request another stream
        conn.toInput.onNext(Message.from(2, MessageType.REQUEST_STREAM, "requestB"));
        // advance some more
        ts.advanceTimeBy(2000, TimeUnit.MILLISECONDS);
        // should have 3 from A and 2 from B
        assertEquals(5, cachedResponses.getValues().length);
        // dispose A, but leave B
        conn.toInput.onNext(Message.from(1, MessageType.CANCEL, ""));
        // still same 5 messages
        assertEquals(5, cachedResponses.getValues().length);
        // advance again, should get 2 from B
        ts.advanceTimeBy(2000, TimeUnit.MILLISECONDS);
        assertEquals(7, cachedResponses.getValues().length);

        List<Message> messages = cachedResponses.take(7).toList().toBlocking().first();

        // A messages (positions 0, 1, 3) incrementing 0, 1, 2
        assertEquals(1, messages.get(0).getStreamId());
        assertEquals("0_requestA", messages.get(0).getMessage());
        assertEquals(1, messages.get(1).getStreamId());
        assertEquals("1_requestA", messages.get(1).getMessage());
        assertEquals(1, messages.get(3).getStreamId());
        assertEquals("2_requestA", messages.get(3).getMessage());

        // B messages (positions 2, 4, 5, 6) incrementing 0, 1, 2, 3
        assertEquals(2, messages.get(2).getStreamId());
        assertEquals("0_requestB", messages.get(2).getMessage());
        assertEquals(2, messages.get(4).getStreamId());
        assertEquals("1_requestB", messages.get(4).getMessage());
        assertEquals(2, messages.get(5).getStreamId());
        assertEquals("2_requestB", messages.get(5).getMessage());
        assertEquals(2, messages.get(6).getStreamId());
        assertEquals("3_requestB", messages.get(6).getMessage());
    }

    /* **********************************************************************************************/

    private ReplaySubject<Message> captureResponses(TestConnection conn) {
        // capture all responses to client
        ReplaySubject<Message> rs = ReplaySubject.create();
        conn.writes.subscribe(rs);
        return rs;
    }

    private TestConnection establishConnection(ReactiveSocketServerProtocol p) {
        TestConnection conn = new TestConnection();
        p.acceptConnection(conn).subscribe(PROTOCOL_SUBSCRIBER);
        return conn;
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

}
