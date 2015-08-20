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

import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;
import io.reactivesocket.Payload;
import io.reactivesocket.RequestHandler;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;

/**
 * Protocol implementation abstracted over a {@link DuplexConnection}.
 * <p>
 * Concrete implementations of {@link DuplexConnection} over TCP, WebSockets, Aeron, etc can be passed to this class for protocol handling. The request handlers passed in at creation will be invoked
 * for each request over the connection.
 */
public class Responder {
	// TODO only handle String right now
	private final RequestHandler requestHandler;

	private Responder(RequestHandler requestHandler) {
		this.requestHandler = requestHandler;
	}

	public static <T> Responder create(RequestHandler requestHandler) {
		return new Responder(requestHandler);
	}

	/**
	 * Accept a new connection and apply ReactiveSocket behavior to it.
	 * 
	 * @param connection
	 * @return Publisher<Void> that must be subscribed to. Receives onError or onComplete, no onNext. Can be unsubscribed to shutdown prematurely.
	 */
	public Publisher<Void> acceptConnection(DuplexConnection connection) {
		/* state of cancellation subjects during connection */
		final Long2ObjectHashMap<Subscription> cancellationSubscriptions = new Long2ObjectHashMap<>();
		/* streams in flight that can receive REQUEST_N messages */
		final Long2ObjectHashMap<?> inFlight = new Long2ObjectHashMap<>(); // TODO not being used

		return new Publisher<Void>() {

			@Override
			public void subscribe(Subscriber<? super Void> child) {
				child.onSubscribe(new Subscription() {

					boolean started = false;
					final AtomicReference<Subscription> transportSubscription = new AtomicReference<>();

					@Override
					public void request(long n) {
						if (!started) {
							started = true;
							// subscribe to transport to get Frames
							connection.getInput().subscribe(new Subscriber<Frame>() {

								@Override
								public void onSubscribe(Subscription s) {
									if (transportSubscription.compareAndSet(null, s)) {
										s.request(Long.MAX_VALUE); // we expect everything from transport (backpressure is via lease/requestN frames)
									} else {
										// means we already were cancelled
										s.cancel();
									}
								}

								@Override
								public void onNext(Frame requestFrame) {
									Publisher<Frame> responsePublisher = null;
									if (requestFrame.getType() == FrameType.REQUEST_RESPONSE) {
										responsePublisher = handleRequestResponse(requestFrame, cancellationSubscriptions);
									} else if (requestFrame.getType() == FrameType.REQUEST_STREAM) {
										responsePublisher = handleRequestStream(requestFrame, cancellationSubscriptions, inFlight);
										// } else if (frame.getType() == FrameType.FIRE_AND_FORGET) {
										// responsePublisher = handleFireAndForget(frame);
										// } else if (frame.getType() == FrameType.REQUEST_SUBSCRIPTION) {
										// responsePublisher = handleRequestSubscription(connection, frame, cancellationSubscriptions, inFlight);
										// } else if (frame.getType() == FrameType.CANCEL) {
										// handleCancellationRequest(cancellationSubscriptions, frame);
										// } else if (requestFrame.getType() == FrameType.REQUEST_N) {
										// handleRequestN(frame, inFlight); // TODO this needs to be implemented
									} else {
										responsePublisher = error(requestFrame, new IllegalStateException("Unexpected prefix: " + requestFrame.getType()));
									}

									connection.addOutput(responsePublisher).subscribe(new Subscriber<Void>() {

										@Override
										public void onSubscribe(Subscription s) {
											s.request(Long.MAX_VALUE); // transport so we request MAX_VALUE
										}

										@Override
										public void onNext(Void t) {
											// nothing expected
										}

										@Override
										public void onError(Throwable t) {
											// TODO all kinds of wrong here ... need to merge with the onComplete on the outer
											child.onError(t);
											cancel();
										}

										@Override
										public void onComplete() {
											// successful completion of IO ... nothing to do as we leave the outer connection open
											// TODO all kinds of wrong here ... need to merge with the onComplete on the outer
										}

									});

								}

								@Override
								public void onError(Throwable t) {
									child.onError(t);
								}

								@Override
								public void onComplete() {
									child.onComplete();
								}

							});
						}
						// REQUEST_N behavior not necessary or expected on this other than to start it
					}

					@Override
					public void cancel() {
						if (!transportSubscription.compareAndSet(null, NOOP_SUBSCRIPTION)) {
							// cancel the one that was there if we failed to set the sentinel
							transportSubscription.get().cancel();
						}
					}

				});

			}

		};
	}

	private Publisher<Frame> handleRequestResponse(
			Frame requestFrame,
			final Long2ObjectHashMap<Subscription> cancellationSubscriptions) {

		return new Publisher<Frame>() {

			@Override
			public void subscribe(Subscriber<? super Frame> child) {
				Subscription s = new Subscription() {

					boolean started = false;
					AtomicReference<Subscription> parent = new AtomicReference<>();

					@Override
					public void request(long n) {
						if (!started) {
							started = true;
							long streamId = requestFrame.getStreamId();

							requestHandler.handleRequestResponse(requestFrame).subscribe(new Subscriber<Payload>() {

								int count = 0;

								@Override
								public void onSubscribe(Subscription s) {
									if (parent.compareAndSet(null, s)) {
										s.request(Long.MAX_VALUE); // only expect 1 value so we don't need REQUEST_N
									} else {
										s.cancel();
										cleanup();
									}
								}

								@Override
								public void onNext(Payload v) {
									if (++count > 1) {
										onError(new IllegalStateException("RequestResponse expects a single onNext"));
									} else {
										child.onNext(Frame.from(streamId, FrameType.NEXT_COMPLETE, v));
									}
								}

								@Override
								public void onError(Throwable t) {
									child.onNext(Frame.from(streamId, t));
									cleanup();
								}

								@Override
								public void onComplete() {
									if (count != 1) {
										onError(new IllegalStateException("RequestResponse expects a single onNext"));
									} else {
										child.onComplete();
										cleanup();
									}
								}

							});
						}
					}

					@Override
					public void cancel() {
						if (!parent.compareAndSet(null, EmptySubscription.EMPTY)) {
							parent.get().cancel();
							cleanup();
						}
					}

					private void cleanup() {
						cancellationSubscriptions.remove(requestFrame.getStreamId());
					}

				};
				cancellationSubscriptions.put(requestFrame.getStreamId(), s);
				child.onSubscribe(s);
			}

		};
	}

	private Publisher<Frame> handleRequestStream(
			Frame requestFrame,
			final Long2ObjectHashMap<Subscription> cancellationSubscriptions,
			final Long2ObjectHashMap<?> inFlight) {

		return new Publisher<Frame>() {

			@Override
			public void subscribe(Subscriber<? super Frame> child) {
				Subscription s = new Subscription() {

					boolean started = false;
					AtomicReference<Subscription> parent = new AtomicReference<>();

					@Override
					public void request(long n) {
						if (!started) {
							started = true;
							long streamId = requestFrame.getStreamId();

							requestHandler.handleRequestStream(requestFrame).subscribe(new Subscriber<Payload>() {

								@Override
								public void onSubscribe(Subscription s) {
									if (parent.compareAndSet(null, s)) {
										s.request(Long.MAX_VALUE); // TODO need backpressure
									} else {
										s.cancel();
										cleanup();
									}
								}

								@Override
								public void onNext(Payload v) {
									child.onNext(Frame.from(streamId, FrameType.NEXT, v));
								}

								@Override
								public void onError(Throwable t) {
									child.onNext(Frame.from(streamId, t));
									cleanup();
								}

								@Override
								public void onComplete() {
									child.onNext(Frame.from(streamId, FrameType.COMPLETE));
									child.onComplete();
									cleanup();
								}

							});
						}
					}

					@Override
					public void cancel() {
						if (!parent.compareAndSet(null, EmptySubscription.EMPTY)) {
							parent.get().cancel();
							cleanup();
						}
					}

					private void cleanup() {
						cancellationSubscriptions.remove(requestFrame.getStreamId());
					}

				};
				cancellationSubscriptions.put(requestFrame.getStreamId(), s);
				child.onSubscribe(s);
			}

		};
	}

	private static final Publisher<Frame> error(Frame requestFrame, Throwable e) {
		return (Subscriber<? super Frame> s) -> {
			s.onSubscribe(new Subscription() {

				@Override
				public void request(long n) {
					// should probably worry about n==0
					s.onNext(Frame.from(requestFrame.getStreamId(), e));
					s.onComplete();
				}

				@Override
				public void cancel() {
					// ignoring just because
				}

			});

		};
	}

	private final static Subscription NOOP_SUBSCRIPTION = new EmptySubscription();

}
