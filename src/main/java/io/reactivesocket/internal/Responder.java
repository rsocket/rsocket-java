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

import io.reactivesocket.*;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivesocket.exceptions.SetupException;
import io.reactivesocket.exceptions.SetupException.SetupErrorCode;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;

/**
 * Protocol implementation abstracted over a {@link DuplexConnection}.
 * <p>
 * Concrete implementations of {@link DuplexConnection} over TCP, WebSockets, Aeron, etc can be passed to this class for protocol handling. The request handlers passed in at creation will be invoked
 * for each request over the connection.
 */
public class Responder {
	private final DuplexConnection connection;
	private final ConnectionSetupHandler connectionHandler;
	private final Consumer<Throwable> errorStream;
	private LeaseGovernor leaseGovernor;

	private Responder(DuplexConnection connection, ConnectionSetupHandler connectionHandler, LeaseGovernor leaseGovernor, Consumer<Throwable> errorStream) {
		this.connection = connection;
		this.connectionHandler = connectionHandler;
		this.leaseGovernor = leaseGovernor;
		this.errorStream = errorStream;
	}

	/**
	 * @param connectionHandler
	 *            Handle connection setup and set up request handling.
	 * @param errorStream
	 *            A {@link Consumer<Throwable>} which will receive all errors that occurs processing requests.
	 *            <p>
	 *            This include fireAndForget which ONLY emit errors server-side via this mechanism.
	 * @return responder instance
	 */
	public static <T> Responder create(DuplexConnection connection, ConnectionSetupHandler connectionHandler, LeaseGovernor leaseGovernor, Consumer<Throwable> errorStream) {
		Responder responder = new Responder(connection, connectionHandler, leaseGovernor, errorStream);
		responder.start();
		return responder;
	}

	/**
	 * Send a LEASE frame immediately. Only way a LEASE is sent. Handled entirely by application logic.
	 *
	 * @param ttl of lease
	 * @param numberOfRequests of lease
	 */
	public void sendLease(final int ttl, final int numberOfRequests)
	{
		connection.addOutput(PublisherUtils.just(Frame.fromLease(ttl, numberOfRequests, Frame.NULL_BYTEBUFFER)), new Completable() {
			@Override
			public void success() {
			}

			@Override
			public void error(Throwable e) {
				errorStream.accept(new RuntimeException("could not send lease ", e));
			}
		});
	}

	private void start() {
		/* state of cancellation subjects during connection */
		final Long2ObjectHashMap<Subscription> cancellationSubscriptions = new Long2ObjectHashMap<>();
		/* streams in flight that can receive REQUEST_N messages */
		final Long2ObjectHashMap<Subscription> inFlight = new Long2ObjectHashMap<>(); // TODO not being used
		/* bidirectional channels */
		final Long2ObjectHashMap<UnicastSubject<Payload>> channels = new Long2ObjectHashMap<>(); // TODO should/can we make this optional so that it only gets allocated per connection if channels are
																									// used?
		final AtomicBoolean childTerminated = new AtomicBoolean(false);
		final AtomicReference<Subscription> transportSubscription = new AtomicReference<>();

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

			RequestHandler requestHandler = null; // null until after first Setup frame

			@Override
			public void onNext(Frame requestFrame) {
				final int streamId = requestFrame.getStreamId();
				if (requestHandler == null) {
					if (childTerminated.get()) {
						// already terminated, but still receiving latent messages ... ignore them while shutdown occurs
						return;
					}
					if (requestFrame.getType().equals(FrameType.SETUP)) {
						final ConnectionSetupPayload connectionSetupPayload = ConnectionSetupPayload.create(requestFrame);
						try {
							requestHandler = connectionHandler.apply(connectionSetupPayload);
						} catch (SetupException setupException) {
							setupErrorAndTearDown(connection, setupException);
						} catch (Throwable e) {
							setupErrorAndTearDown(connection, new SetupException(SetupErrorCode.REJECTED, e));
						}

						// the L bit set must wait until the application logic explicitly sends
						// a LEASE. ConnectionSetupPlayload knows of bits being set.
						if (connectionSetupPayload.willClientHonorLease()) {
							leaseGovernor.register(Responder.this);
						} else {
							leaseGovernor = LeaseGovernor.UNLIMITED_LEASE_GOVERNOR;
						}

						// TODO: handle keepalive logic here
					} else {
						setupErrorAndTearDown(connection, new SetupException(SetupErrorCode.INVALID_SETUP, "Setup frame missing"));
					}
				} else {
					Publisher<Frame> responsePublisher = null;
					if (leaseGovernor.accept(Responder.this, requestFrame)) {
						try {
							if (requestFrame.getType() == FrameType.REQUEST_RESPONSE) {
								responsePublisher = handleRequestResponse(requestFrame, requestHandler, cancellationSubscriptions);
							} else if (requestFrame.getType() == FrameType.REQUEST_STREAM) {
								responsePublisher = handleRequestStream(requestFrame, requestHandler, cancellationSubscriptions, inFlight);
							} else if (requestFrame.getType() == FrameType.FIRE_AND_FORGET) {
								responsePublisher = handleFireAndForget(requestFrame, requestHandler);
							} else if (requestFrame.getType() == FrameType.REQUEST_SUBSCRIPTION) {
								responsePublisher = handleRequestSubscription(requestFrame, requestHandler, cancellationSubscriptions, inFlight);
							} else if (requestFrame.getType() == FrameType.REQUEST_CHANNEL) {
								responsePublisher = handleRequestChannel(requestFrame, requestHandler, channels, cancellationSubscriptions, inFlight);
							} else if (requestFrame.getType() == FrameType.CANCEL) {
								Subscription s = null;
								synchronized (Responder.this) {
									s = cancellationSubscriptions.get(requestFrame.getStreamId());
								}
								if (s != null) {
									s.cancel();
								}
								return;
							} else if (requestFrame.getType() == FrameType.REQUEST_N) {
								Subscription inFlightSubscription = null;
								synchronized (Responder.this) {
									inFlightSubscription = inFlight.get(requestFrame.getStreamId());
								}
								if (inFlightSubscription != null) {
									inFlightSubscription.request(Frame.RequestN.requestN(requestFrame));
									return;
								}
								// TODO should we do anything if we don't find the stream? emitting an error is risky as the responder could have terminated and cleaned up already
							} else {
								responsePublisher = PublisherUtils.errorFrame(streamId, new IllegalStateException("Unexpected prefix: " + requestFrame.getType()));
							}
						} catch (Throwable e) {
							// synchronous try/catch since we execute user functions in the handlers and they could throw
							errorStream.accept(new RuntimeException("Error in request handling.", e));
							// error message to user
							responsePublisher = PublisherUtils.errorFrame(streamId, new RuntimeException("Unhandled error processing request"));
						}
					} else {
//						responsePublisher = PublisherUtils.errorFrame(streamId, new LeaseException());
						responsePublisher = PublisherUtils.errorFrame(streamId, new RuntimeException("Lease Exception"));
					}
					connection.addOutput(responsePublisher, new Completable() {

						@Override
						public void success() {
							// TODO Auto-generated method stub

						}

						@Override
						public void error(Throwable e) {
							// TODO validate with unit tests
							if (childTerminated.compareAndSet(false, true)) {
								errorStream.accept(new RuntimeException("Error writing", e)); // TODO should we have typed RuntimeExceptions?
								cancel();
							}
						}

					});
				}
			}

			private void setupErrorAndTearDown(DuplexConnection connection, SetupException setupException) {
				// pass the ErrorFrame output, subscribe to write it, await onComplete and then tear down
				connection.addOutput(PublisherUtils.just(Frame.fromSetupError(setupException.getErrorCode().getCode(), "", setupException.getMessage())),
						new Completable() {

					@Override
					public void success() {
						tearDownWithError(setupException);
					}

					@Override
					public void error(Throwable e) {
						tearDownWithError(new RuntimeException("Failure outputting SetupException", e));
					}

				});
			}

			private void tearDownWithError(Throwable se) {
				onError(new RuntimeException("Connection Setup Failure", se)); // TODO unit test that this actually shuts things down
			}

			@Override
			public void onError(Throwable t) {
				// TODO validate with unit tests
				if (childTerminated.compareAndSet(false, true)) {
					errorStream.accept(t);
					cancel();
				}
			}

			@Override
			public void onComplete() {
				//TODO validate what is happening here
				// this would mean the connection gracefully shut down, which is unexpected
				if (childTerminated.compareAndSet(false, true)) {
					cancel();
				}
			}
			
			private void cancel() {
				// child has cancelled (shutdown the connection or server) // TODO validate with unit tests
				if (!transportSubscription.compareAndSet(null, EmptySubscription.EMPTY)) {
					// cancel the one that was there if we failed to set the sentinel
					transportSubscription.get().cancel();
				}
			}

		});
	}


	private Publisher<Frame> handleRequestResponse(
			Frame requestFrame,
			final RequestHandler requestHandler,
			final Long2ObjectHashMap<Subscription> cancellationSubscriptions) {

		return (Subscriber<? super Frame> child) -> {
			Subscription s = new Subscription() {

				boolean started = false;
				AtomicReference<Subscription> parent = new AtomicReference<>();

				@Override
				public void request(long n) {
					if (!started) {
						started = true;
						final int streamId = requestFrame.getStreamId();

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
								child.onNext(Frame.fromError(streamId, t));
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
					synchronized(Responder.this) {
						cancellationSubscriptions.remove(requestFrame.getStreamId());
					}
				}

			};
			synchronized(Responder.this) {
				cancellationSubscriptions.put(requestFrame.getStreamId(), s);
			}
			child.onSubscribe(s);
		};
	}

	private static BiFunction<RequestHandler, Payload, Publisher<Payload>> requestSubscriptionHandler = (RequestHandler handler, Payload requestPayload) -> handler
			.handleSubscription(requestPayload);
	private static BiFunction<RequestHandler, Payload, Publisher<Payload>> requestStreamHandler = (RequestHandler handler, Payload requestPayload) -> handler.handleRequestStream(requestPayload);

	private Publisher<Frame> handleRequestStream(
			Frame requestFrame,
			final RequestHandler requestHandler,
			final Long2ObjectHashMap<Subscription> cancellationSubscriptions,
			final Long2ObjectHashMap<Subscription> inFlight) {
		return requestStream(requestStreamHandler, requestFrame, requestHandler, cancellationSubscriptions, inFlight, true);
	}

	private Publisher<Frame> handleRequestSubscription(
			Frame requestFrame,
			final RequestHandler requestHandler,
			final Long2ObjectHashMap<Subscription> cancellationSubscriptions,
			final Long2ObjectHashMap<Subscription> inFlight) {
		return requestStream(requestSubscriptionHandler, requestFrame, requestHandler, cancellationSubscriptions, inFlight, false);
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
			final RequestHandler requestHandler,
			final Long2ObjectHashMap<Subscription> cancellationSubscriptions,
			final Long2ObjectHashMap<Subscription> inFlight,
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
							final int streamId = requestFrame.getStreamId();

							handler.apply(requestHandler, requestFrame).subscribe(new Subscriber<Payload>() {

								@Override
								public void onSubscribe(Subscription s) {
									if (parent.compareAndSet(null, s)) {
										inFlight.put(streamId, s);
										s.request(Frame.Request.initialRequestN(requestFrame));
									} else {
										s.cancel();
										cleanup();
									}
								}

								@Override
								public void onNext(Payload v) {
									try {
										child.onNext(Frame.from(streamId, FrameType.NEXT, v));
									} catch (Throwable e) {
										onError(e);
									}
								}

								@Override
								public void onError(Throwable t) {
									child.onNext(Frame.fromError(streamId, t));
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
						synchronized(Responder.this) {
							inFlight.remove(requestFrame.getStreamId());
							cancellationSubscriptions.remove(requestFrame.getStreamId());
						}
					}

				};
				synchronized(Responder.this) {
					cancellationSubscriptions.put(requestFrame.getStreamId(), s);
				}
				child.onSubscribe(s);
			}

		};

	}

	private Publisher<Frame> handleFireAndForget(Frame requestFrame, final RequestHandler requestHandler) {
		try {
			requestHandler.handleFireAndForget(requestFrame).subscribe(fireAndForgetSubscriber);
		} catch (Throwable e) {
			// we catch these errors here as we don't want anything propagating back to the user on fireAndForget
			errorStream.accept(new RuntimeException("Error processing 'fireAndForget'", e));
		}
		return PublisherUtils.empty(); // we always treat this as if it immediately completes as we don't want errors passing back to the user
	}

	/**
	 * Reusable for each fireAndForget since no state is shared across invocations. It just passes through errors.
	 */
	private final Subscriber<Void> fireAndForgetSubscriber = new Subscriber<Void>(){

	@Override public void onSubscribe(Subscription s){s.request(Long.MAX_VALUE);}

	@Override public void onNext(Void t){}

	@Override public void onError(Throwable t){errorStream.accept(t);}

	@Override public void onComplete(){}

	};

	private Publisher<Frame> handleRequestChannel(Frame requestFrame,
			RequestHandler requestHandler,
			Long2ObjectHashMap<UnicastSubject<Payload>> channels,
			Long2ObjectHashMap<Subscription> cancellationSubscriptions,
			Long2ObjectHashMap<Subscription> inFlight) {

		UnicastSubject<Payload> channelSubject = null;
		synchronized(Responder.this) {
			channelSubject = channels.get(requestFrame.getStreamId());
		}
		if (channelSubject == null) {
			// first request on this channel
			channelSubject = UnicastSubject.create((s, rn) -> {
				// after we are first subscribed to then send the initial frame
				s.onNext(requestFrame);
			});
			synchronized(Responder.this) {
				channels.put(requestFrame.getStreamId(), channelSubject);
			}

			final UnicastSubject<Payload> channelRequests = channelSubject;

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
								final int streamId = requestFrame.getStreamId();

								requestHandler.handleChannel(requestFrame, channelRequests).subscribe(new Subscriber<Payload>() {

									@Override
									public void onSubscribe(Subscription s) {
										if (parent.compareAndSet(null, s)) {
											inFlight.put(streamId, s);
											s.request(Frame.Request.initialRequestN(requestFrame));
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
										child.onNext(Frame.fromError(streamId, t));
										child.onComplete();
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
							synchronized(Responder.this) {
								inFlight.remove(requestFrame.getStreamId());
								cancellationSubscriptions.remove(requestFrame.getStreamId());
							}
						}

					};
					synchronized(Responder.this) {
						cancellationSubscriptions.put(requestFrame.getStreamId(), s);
					}
					child.onSubscribe(s);
				}

			};

		} else {
			// send data to channel
			if (channelSubject.isSubscribedTo()) {
				channelSubject.onNext(requestFrame); // TODO this is ignoring requestN flow control (need to validate that this is legit because REQUEST_N across the wire is controlling it on the Requester side)
				// TODO should at least have an error message of some kind if the Requester disregarded it
				return PublisherUtils.empty();
			} else {
				// TODO should we use a BufferUntilSubscriber solution instead to handle time-gap issues like this?
				return PublisherUtils.errorFrame(requestFrame.getStreamId(), new RuntimeException("Channel unavailable")); // TODO validate with unit tests.
			}
		}
	}

}
