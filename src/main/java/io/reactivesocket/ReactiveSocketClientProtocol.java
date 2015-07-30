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

import static rx.Observable.empty;
import static rx.Observable.error;
import static rx.Observable.just;
import static rx.RxReactiveStreams.toObservable;
import static rx.RxReactiveStreams.toPublisher;

import java.util.concurrent.atomic.AtomicBoolean;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import rx.Observable;
import rx.Producer;
import rx.observables.ConnectableObservable;
import rx.observers.Subscribers;
import rx.subjects.PublishSubject;
import rx.subscriptions.Subscriptions;

/**
 * Client-side protocol implementation abstracted over a {@link DuplexConnection}.
 * <p>
 * Concrete implementations of {@link DuplexConnection} over TCP, WebSockets, Aeron, etc
 * can be passed to this class for protocol handling.
 */
public class ReactiveSocketClientProtocol {

    // TODO replace String with whatever ByteBuffer/byte[]/ByteBuf/etc variant we choose

    private final DuplexConnection connection;
    /**
     * TODO determine if Subject with multicast+filter is better or
     * a Map each containing a single Subject for lookup+delivery is better
     * for doing demux ... currently chosen multicast+filter
     */
    private final Observable<Frame> multicastedInputStream;
    private int streamCount = 0;// 0 is reserved for setup, all normal messages are >= 1

    private ReactiveSocketClientProtocol(DuplexConnection connection) {
        this.connection = connection;
        // multicast input stream to any requestor (with backpressure support via RxJava .publish() operator)
        ConnectableObservable<Frame> published = toObservable(connection.getInput()).publish();
        multicastedInputStream = published;
        // start listening ... ignoring returned subscription as we don't control connection lifecycle here
        published.connect();
    }

    public static ReactiveSocketClientProtocol create(DuplexConnection connection) {
        return new ReactiveSocketClientProtocol(connection);
    }

    /**
     * Request/Response with a single message response.
     * 
     * @param payload
     * @return
     */
    public Publisher<String> requestResponse(String payload) {
        return startStream(Frame.from(nextStreamId(), FrameType.REQUEST_RESPONSE, payload));
    }

    /**
     * Request/Stream with a finite multi-message response followed by a terminal
     * state {@link Subscriber#onComplete()} or {@link Subscriber#onError(Throwable)}.
     * 
     * @param payload
     * @return
     */
    public Publisher<String> requestStream(String payload) {
        return startStream(Frame.from(nextStreamId(), FrameType.REQUEST_STREAM, payload));
    }

    /**
     * Fire-and-forget without a response from the server.
     * <p>
     * The returned {@link Publisher} will emit {@link Subscriber#onComplete()} or
     * {@link Subscriber#onError(Throwable)} to represent success or failure in sending
     * from the client side, but no feedback from the server will be returned.
     * 
     * @param payload
     * @return
     */
    public Publisher<Void> fireAndForget(String payload) {
        return connection.write(toPublisher(just(Frame.from(nextStreamId(), FrameType.FIRE_AND_FORGET, payload))));
    }

    /**
     * Event subscription with an infinite multi-message response potentially
     * terminated with an {@link Subscriber#onError(Throwable)}.
     * 
     * @param payload
     * @return
     */
    public Publisher<String> requestSubscription(String payload) {
        return startStream(Frame.from(nextStreamId(), FrameType.REQUEST_SUBSCRIPTION, payload));
    }

    private Publisher<String> startStream(Frame requestFrame) {
        return toPublisher(Observable.create(child -> {

            // TODO replace this with a UnicastSubject without the overhead of multicast support
            PublishSubject<Observable<Frame>> writer = PublishSubject.create();
            Observable<Void> written = toObservable(connection.write(toPublisher(Observable.merge(writer))));

            child.setProducer(new Producer() {

                private final AtomicBoolean started = new AtomicBoolean(false);

                @Override
                public void request(long n) {
                    if (started.compareAndSet(false, true)) {
                        start();
                    }
                    // send REQUEST_N over network for streaming request types
                    if (requestFrame.getMessageType() == FrameType.REQUEST_STREAM || requestFrame.getMessageType() == FrameType.REQUEST_SUBSCRIPTION) {
                        writer.onNext(just(Frame.from(requestFrame.getStreamId(), FrameType.REQUEST_N, String.valueOf(n))));
                    }
                }

                private void start() {
                    // wire up the response handler before emitting request
                    Observable<Frame> input = multicastedInputStream
                            .filter(m -> m.getStreamId() == requestFrame.getStreamId());

                    // combine input and output so errors and unsubscription are composed, then subscribe
                    rx.Subscription subscription = Observable
                            .merge(input, written.cast(Frame.class))
                            .takeUntil(m -> (m.getMessageType() == FrameType.COMPLETE
                                    || m.getMessageType() == FrameType.ERROR))
                            .flatMap(m -> {
                        // convert ERROR/COMPLETE messages into terminal events
                        if (m.getMessageType() == FrameType.ERROR) {
                            return error(new Exception(m.getMessage()));
                        } else if (m.getMessageType() == FrameType.COMPLETE) {
                            return empty();// unsubscribe handled in takeUntil above
                        } else if (m.getMessageType() == FrameType.NEXT) {
                            return just(m.getMessage());
                        } else {
                            return error(new Exception("Unexpected FrameType: " + m.getMessageType()));
                        }
                    }).subscribe(Subscribers.from(child));// only propagate Observer methods, backpressure is via Producer above

                    // if the child unsubscribes, we need to send a CANCEL message if we're not terminated
                    child.add(Subscriptions.create(() -> {
                        writer.onNext(just(Frame.from(requestFrame.getStreamId(), FrameType.CANCEL, "")));
                        // after sending the CANCEL we then tear down this stream
                        subscription.unsubscribe();
                    }));

                    // send the request to start everything
                    writer.onNext(just(requestFrame));
                }

            });

        }));
    }

    private int nextStreamId() {
        // use ++ prefix so streamCount always equals the last stream created
        return ++streamCount;
    }

}
