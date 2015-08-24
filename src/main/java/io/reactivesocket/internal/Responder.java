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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;

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
	private final RequestHandler requestHandler;
	private final Consumer<Throwable> errorStream;

	private Responder(RequestHandler requestHandler, Consumer<Throwable> errorStream) {
		this.requestHandler = requestHandler;
		this.errorStream = errorStream;
	}

	/**
	 * @param requestHandler
	 *            Handle incoming requests.
	 * @return
	 */
	public static <T> Responder create(RequestHandler requestHandler) {
		return new Responder(requestHandler, t -> {
		});
	}

	/**
	 * @param requestHandler
	 *            Handle incoming requests.
	 * @param errorStream
	 *            A {@link Consumer<Throwable>} which will receive all errors that occurs processing requests.
	 *            <p>
	 *            This include fireAndForget which ONLY emit errors server-side via this mechanism.
	 * @return
	 */
	public static <T> Responder create(RequestHandler requestHandler, Consumer<Throwable> errorStream) {
		return new Responder(requestHandler, errorStream);
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
				final AtomicBoolean childTerminated = new AtomicBoolean(false);
				child.onSubscribe(new Subscription() {

					boolean started = false;
					final AtomicReference<Subscription> transportSubscription = new AtomicReference<>();

					@Override
					public void request(long n) {
						// REQUEST_N behavior not necessary or expected on this other than to start it
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
									try {
										if (requestFrame.getType() == FrameType.REQUEST_RESPONSE) {
											responsePublisher = handleRequestResponse(requestFrame, cancellationSubscriptions);
										} else if (requestFrame.getType() == FrameType.REQUEST_STREAM) {
											responsePublisher = handleRequestStream(requestFrame, cancellationSubscriptions, inFlight);
										} else if (requestFrame.getType() == FrameType.FIRE_AND_FORGET) {
											responsePublisher = handleFireAndForget(requestFrame);
										} else if (requestFrame.getType() == FrameType.REQUEST_SUBSCRIPTION) {
											responsePublisher = handleRequestSubscription(requestFrame, cancellationSubscriptions, inFlight);
										} else if (requestFrame.getType() == FrameType.CANCEL) {
											Subscription s = cancellationSubscriptions.get(requestFrame.getStreamId());
											if (s != null) {
												s.cancel();
											}
											return;
										} else if (requestFrame.getType() == FrameType.REQUEST_N) {
											// TODO do something with this
											return;
										} else {
											responsePublisher = PublisherUtils.error(requestFrame, new IllegalStateException("Unexpected prefix: " + requestFrame.getType()));
										}
									} catch (Throwable e) {
										// synchronous try/catch since we execute user functions in the handlers and they could throw
										errorStream.accept(new RuntimeException("Error in request handling.", e));
										// error message to user
										responsePublisher = PublisherUtils.error(requestFrame, new RuntimeException("Unhandled error processing request"));
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
											// TODO validate with unit tests
											errorStream.accept(new RuntimeException("Error writing", t)); // TODO should we have typed RuntimeExceptions?
											if (childTerminated.compareAndSet(false, true)) {
												child.onError(t);
												cancel();
											}
										}

										@Override
										public void onComplete() {
											// successful completion of write IO
											// We don't need to do anything here onComplete. If an error occurs,
											// or the outer connection shuts down it will terminate any IO still happening here and cleanup.
										}

									});

								}

								@Override
								public void onError(Throwable t) {
									// TODO validate with unit tests
									if (childTerminated.compareAndSet(false, true)) {
										child.onError(t);
										cancel();
									}
								}

								@Override
								public void onComplete() {
									// this would mean the connection gracefully shut down, which is unexpected
									if (childTerminated.compareAndSet(false, true)) {
										child.onComplete();
										cancel();
									}
								}

							});
						}
					}

					@Override
					public void cancel() {
						// child has cancelled (shutdown the connection or server)  // TODO validate with unit tests
						if (!transportSubscription.compareAndSet(null, EmptySubscription.EMPTY)) {
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

	private static BiFunction<RequestHandler, Payload, Publisher<Payload>> requestSubscriptionHandler = (RequestHandler handler, Payload requestPayload) -> handler
			.handleRequestSubscription(requestPayload);
	private static BiFunction<RequestHandler, Payload, Publisher<Payload>> requestStreamHandler = (RequestHandler handler, Payload requestPayload) -> handler.handleRequestStream(requestPayload);

	private Publisher<Frame> handleRequestStream(
			Frame requestFrame,
			final Long2ObjectHashMap<Subscription> cancellationSubscriptions,
			final Long2ObjectHashMap<?> inFlight) {
		return requestStream(requestStreamHandler, requestFrame, cancellationSubscriptions, inFlight, true);
	}

	private Publisher<Frame> handleRequestSubscription(
			Frame requestFrame,
			final Long2ObjectHashMap<Subscription> cancellationSubscriptions,
			final Long2ObjectHashMap<?> inFlight) {
		return requestStream(requestSubscriptionHandler, requestFrame, cancellationSubscriptions, inFlight, false);
	}

	/**
	 * Common logic for requestStream and requestSubscription
	 * 
	 * @param handler
	 * @param requestFrame
	 * @param cancellationSubscriptions
	 * @param inFlight
	 * @param allowCompletion
	 * @return
	 */
	private Publisher<Frame> requestStream(
			BiFunction<RequestHandler, Payload, Publisher<Payload>> handler,
			Frame requestFrame,
			final Long2ObjectHashMap<Subscription> cancellationSubscriptions,
			final Long2ObjectHashMap<?> inFlight,
			final boolean allowCompletion) {

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

							handler.apply(requestHandler, requestFrame).subscribe(new Subscriber<Payload>() {

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
									child.onComplete();
									cleanup();
								}

								@Override
								public void onComplete() {
									if (allowCompletion) {
										child.onNext(Frame.from(streamId, FrameType.COMPLETE));
										child.onComplete();
										cleanup();
									} else {
										onError(new IllegalStateException("Unexpected onComplete occurred on 'requestSubscription'"));
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

	/**
	 * Reusable for each fireAndForget since no state is shared across invocations. It just passes through errors.
	 */
	private final Subscriber<Void> fireAndForgetSubscriber = new Subscriber<Void>() {

		@Override
		public void onSubscribe(Subscription s) {
			s.request(Long.MAX_VALUE);
		}

		@Override
		public void onNext(Void t) {
		}

		@Override
		public void onError(Throwable t) {
			errorStream.accept(t);
		}

		@Override
		public void onComplete() {
		}

	};

	private Publisher<Frame> handleFireAndForget(Frame requestFrame) {
		try {
			requestHandler.handleFireAndForget(requestFrame).subscribe(fireAndForgetSubscriber);
		} catch (Throwable e) {
			// we catch these errors here as we don't want anything propagating back to the user on fireAndForget
			errorStream.accept(new RuntimeException("Error processing 'fireAndForget'", e));
		}
		return PublisherUtils.empty(); // we always treat this as if it immediately completes as we don't want errors passing back to the user
	}

}
