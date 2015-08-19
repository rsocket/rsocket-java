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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;
import io.reactivesocket.Payload;
import rx.Observable;
import rx.Producer;
import rx.observers.Subscribers;
import rx.subjects.PublishSubject;
import rx.subscriptions.Subscriptions;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import static rx.Observable.just;
import static rx.RxReactiveStreams.toObservable;
import static rx.RxReactiveStreams.toPublisher;

/**
 * Protocol implementation abstracted over a {@link DuplexConnection}.
 * <p>
 * Concrete implementations of {@link DuplexConnection} over TCP, WebSockets, Aeron, etc
 * can be passed to this class for protocol handling.
 */
public class Requester {
	
	private final boolean isServer;
    private final DuplexConnection connection;
    private final Long2ObjectHashMap<UnicastSubject> streamInputMap = new Long2ObjectHashMap<>();
    private int streamCount = 0;// 0 is reserved for setup, all normal messages are >= 1

    private Requester(boolean isServer, DuplexConnection connection) {
    	this.isServer = isServer;
        this.connection = connection;
    }

    /**
     * Create a Requester for each DuplexConnection that requests will be made over.
     * <p>
     * NOTE: You must start().subscribe() the Requester for it to run.
     * 
     * @param isServer for debugging purposes
     * @param connection
     * @return
     */
    public static Requester createForConnection(boolean isServer, DuplexConnection connection) {
        return new Requester(isServer, connection);
    }
    
    public static Requester createClientRequester(DuplexConnection connection) {
        return new Requester(false, connection);
    }
    
    public static Requester createServerRequester(DuplexConnection connection) {
        return new Requester(true, connection);
    }

    /**
     * Request/Response with a single message response.
     * 
     * @param data
     * @return
     */
    public Publisher<Payload> requestResponse(final Payload payload) {
        return startStream(Frame.from(nextStreamId(), FrameType.REQUEST_RESPONSE, payload));
    }

    /**
     * Request/Stream with a finite multi-message response followed by a terminal
     * state {@link Subscriber#onComplete()} or {@link Subscriber#onError(Throwable)}.
     * 
     * @param data
     * @return
     */
    public Publisher<Payload> requestStream(final Payload payload) {
        return startStream(Frame.from(nextStreamId(), FrameType.REQUEST_STREAM, payload));
    }

    /**
     * Fire-and-forget without a response from the server.
     * <p>
     * The returned {@link Publisher} will emit {@link Subscriber#onComplete()} or
     * {@link Subscriber#onError(Throwable)} to represent success or failure in sending
     * from the client side, but no feedback from the server will be returned.
     * 
     * @param data
     * @return
     */
    public Publisher<Void> fireAndForget(final Payload payload) {
        return connection.write(toPublisher(just(Frame.from(nextStreamId(), FrameType.FIRE_AND_FORGET, payload))));
    }

    /**
     * Event subscription with an infinite multi-message response potentially
     * terminated with an {@link Subscriber#onError(Throwable)}.
     * 
     * @param data
     * @return
     */
    public Publisher<Payload> requestSubscription(final Payload payload) {
        return startStream(Frame.from(nextStreamId(), FrameType.REQUEST_SUBSCRIPTION, payload));
    }

    private Publisher<Payload> startStream(Frame requestFrame) {
        return toPublisher(Observable.create(child -> {

        	System.out.println("[" + (isServer ? "Server" : "Client") + "] " + " REQUESTOR.startStream " + requestFrame);
        	streamInputMap.put(requestFrame.getStreamId(), UnicastSubject.create());
            PublishSubject<Observable<Frame>> writer = PublishSubject.create();
            writer.flatMap(i -> i).forEach(n -> System.out.println("[" + (isServer ? "Server" : "Client") + "] " + " REQUESTOR.write.toServer => " + n));
            
            Observable<Void> written = toObservable(connection.write(toPublisher(Observable.merge(writer))));

            child.setProducer(new Producer() {

                private final AtomicBoolean started = new AtomicBoolean(false);

                @Override
                public void request(long n) {
                    if (started.compareAndSet(false, true)) {
                        start(n);
                    }
                    else if (requestFrame.getType() == FrameType.REQUEST_STREAM || requestFrame.getType() == FrameType.REQUEST_SUBSCRIPTION) {
                        writer.onNext(just(Frame.from(requestFrame.getStreamId(), FrameType.REQUEST_N)));
                    }
                }

                private void start(final long n) {
                    // wire up the response handler before emitting request
                	Observable<Frame> input = streamInputMap.get(requestFrame.getStreamId());
                    AtomicBoolean terminated = new AtomicBoolean(false);
                    // combine input and output so errors and unsubscription are composed, then subscribe
                    rx.Subscription subscription = Observable
                        .merge(input, written.cast(Frame.class))
                        .doOnNext(f -> System.out.println("[" + (isServer ? "Server" : "Client") + "] " + " REQUESTOR onNext " + f))
                        .takeUntil(frame -> (frame.getType() == FrameType.COMPLETE
                            || frame.getType() == FrameType.ERROR)
                            || frame.getType() == FrameType.NEXT_COMPLETE)
                        .filter(frame -> frame.getType() != FrameType.COMPLETE)
                        .map(frame -> {
                        	System.out.println("[" + (isServer ? "Server" : "Client") + "] " + " REQUESTOR frame: " + frame);
                            // convert ERROR messages into terminal events
                            if (frame.getType() == FrameType.NEXT)
                            {
                                return frame;
                            }
                            else if (frame.getType() == FrameType.NEXT_COMPLETE)
                            {
                                terminated.set(true);
                                return frame;
                            }
                            else if (frame.getType() == FrameType.ERROR)
                            {
                                terminated.set(true);
                                final ByteBuffer byteBuffer = frame.getData();
                                final byte[] bytes = new byte[byteBuffer.capacity()];
                                byteBuffer.get(bytes);

                                throw new RuntimeException(new String(bytes));
                            }
                            else
                            {
                                throw new RuntimeException("Unexpected FrameType: " + frame.getType());
                            }
                        })
                        .subscribe(Subscribers.from(child));// only propagate Observer methods, backpressure is via Producer above

                    // if the child unsubscribes, we need to send a CANCEL message if we're not terminated
                    child.add(Subscriptions.create(() -> {
                        if (!terminated.get()) {
                            writer.onNext(just(Frame.from(requestFrame.getStreamId(), FrameType.CANCEL)));
                        }
                        // after sending the CANCEL we then tear down this stream
                        subscription.unsubscribe();
                        streamInputMap.remove(requestFrame.getStreamId());
                    }));

                    System.out.println("[" + (isServer ? "Server" : "Client") + "] " + " REQUESTOR write request: " + requestFrame);
                    // send the request to start everything
                    // TODO: modify requestFrame initial request N with 'n' value that was passed in
                    writer.onNext(just(requestFrame));
                }

            });

        }));
    }

    private int nextStreamId() {
        // use ++ prefix so streamCount always equals the last stream created
        return ++streamCount;
    }

	public Publisher<Void> start() {
		return toPublisher(Observable.create(terminalObserver -> {
			// get input from responder->requestor for responses
			connection.getInput().subscribe(new Subscriber<Frame>() {
				public void onSubscribe(Subscription s) {
					s.request(Long.MAX_VALUE);
				}

				public void onNext(Frame frame) {
					System.out.println("[" + (isServer ? "Server" : "Client") + "] " + " REQUESTOR request type being routed to inputStream " + frame);
					streamInputMap.get(frame.getStreamId()).onNext(frame);
				}

				public void onError(Throwable t) {
					streamInputMap.forEach((id, subject) -> subject.onError(t));
					// TODO: iterate over responder side and destroy world
					terminalObserver.onError(t);
				}

				public void onComplete() {
					// TODO: might be a RuntimeException
					streamInputMap.forEach((id, subject) -> subject.onCompleted());
					// TODO: iterate over responder side and destroy world
					terminalObserver.onCompleted();
				}
			});
		}));
	}

}
