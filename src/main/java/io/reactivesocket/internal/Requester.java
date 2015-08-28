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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import io.reactivesocket.exceptions.LeaseException;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivesocket.Completable;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;
import io.reactivesocket.Payload;
import io.reactivesocket.exceptions.SetupException;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;

/**
 * Protocol implementation abstracted over a {@link DuplexConnection}.
 * <p>
 * Concrete implementations of {@link DuplexConnection} over TCP, WebSockets, Aeron, etc can be passed to this class for protocol handling.
 */
public class Requester {

	private final static Subscription CANCELLED = new EmptySubscription();

	private final boolean isServer;
	private final DuplexConnection connection;
	private final Long2ObjectHashMap<UnicastSubject<Frame>> streamInputMap = new Long2ObjectHashMap<>();
	private final ConnectionSetupPayload setupPayload;
	private final Consumer<Throwable> errorStream;
	private final boolean honorLease;

	private long ttlExpiration;
	private long numberOfRemainingRequests = 0;
	private long streamCount = 0; // 0 is reserved for setup, all normal messages are >= 1

	private Requester(boolean isServer, DuplexConnection connection, ConnectionSetupPayload setupPayload, Consumer<Throwable> errorStream) {
		this.isServer = isServer;
		this.connection = connection;
		this.setupPayload = setupPayload;
		this.errorStream = errorStream;
		if (isServer) {
			streamCount = 1; // server is odds
		} else {
			streamCount = 0; // client is even
		}

		if (setupPayload.willClientHonorLease())
		{
			this.honorLease = true;
		}
		else
		{
			this.honorLease = false;
		}
	}

	public static Requester createClientRequester(DuplexConnection connection, ConnectionSetupPayload setupPayload, Consumer<Throwable> errorStream) {
		Requester requester = new Requester(false, connection, setupPayload, errorStream);
		requester.start();
		return requester;
	}

	public static Requester createServerRequester(DuplexConnection connection, Consumer<Throwable> errorStream) {
		Requester requester = new Requester(true, connection, null, errorStream);
		requester.start();
		return requester;
	}

	public boolean isServer() {
		return isServer;
	}

	/**
	 * Request/Response with a single message response.
	 * 
	 * @param data
	 * @return
	 */
	public Publisher<Payload> requestResponse(final Payload payload) {
		return startStream(nextStreamId(), FrameType.REQUEST_RESPONSE, payload);
	}

	/**
	 * Request/Stream with a finite multi-message response followed by a terminal state {@link Subscriber#onComplete()} or {@link Subscriber#onError(Throwable)}.
	 * 
	 * @param data
	 * @return
	 */
	public Publisher<Payload> requestStream(final Payload payload) {
		return startStream(nextStreamId(), FrameType.REQUEST_STREAM, payload);
	}

	/**
	 * Fire-and-forget without a response from the server.
	 * <p>
	 * The returned {@link Publisher} will emit {@link Subscriber#onComplete()} or {@link Subscriber#onError(Throwable)} to represent success or failure in sending from the client side, but no
	 * feedback from the server will be returned.
	 * 
	 * @param data
	 * @return
	 */
	public Publisher<Void> fireAndForget(final Payload payload) {
		if (payload == null) {
			throw new IllegalStateException("Payload can not be null");
		}
		// TODO assert connection is ready (after start() and SETUP frame is sent)

		return new Publisher<Void>() {

			@Override
			public void subscribe(Subscriber<? super Void> child) {
				child.onSubscribe(new Subscription() {

					boolean started = false;

					@Override
					public void request(long n) {
						if (!started && n > 0) {
							started = true;

							// does lease allow for sending request
							if (!canRequest())
							{
								child.onError(new LeaseException());
							}
							else
							{
								numberOfRemainingRequests--;
							}

							connection.addOutput(PublisherUtils.just(Frame.fromRequest(nextStreamId(), FrameType.FIRE_AND_FORGET, payload, 0)), new Completable() {

								@Override
								public void success() {
									child.onComplete();
								}

								@Override
								public void error(Throwable e) {
									child.onError(e);
								}

							});
						}
					}

					@Override
					public void cancel() {
						// nothing to cancel on a fire-and-forget
					}

				});
			}

		};
	}

	/**
	 * Event subscription with an infinite multi-message response potentially terminated with an {@link Subscriber#onError(Throwable)}.
	 * 
	 * @param data
	 * @return
	 */
	public Publisher<Payload> requestSubscription(final Payload payload) {
		return startStream(nextStreamId(), FrameType.REQUEST_SUBSCRIPTION, payload);
	}

	/**
	 * Request/Stream with a finite multi-message response followed by a terminal state {@link Subscriber#onComplete()} or {@link Subscriber#onError(Throwable)}.
	 * 
	 * @param data
	 * @return
	 */
	public Publisher<Payload> requestChannel(final Publisher<Payload> payloadStream) {
		return startStream(nextStreamId(), FrameType.REQUEST_CHANNEL, payloadStream);
	}

	/**
	 * Return availability of sending requests
	 *
	 * @return
	 */
	public double availability()
	{
		return (canRequest() ? 1.0 : 0.0);
	}

	/**
	 * Start a stream with a single request Payload.
	 * 
	 * @param streamId
	 * @param type
	 * @param payload
	 * @return
	 */
	private Publisher<Payload> startStream(long streamId, FrameType type, Payload payload) {
		if (payload == null) {
			throw new IllegalStateException("Payload can not be null");
		}
		if (type == null) {
			throw new IllegalStateException("FrameType can not be null");
		}
		return startStream(streamId, type, payload, null);
	}

	/**
	 * Start a bi-directional stream supporting multiple request Payloads.
	 * 
	 * @param streamId
	 * @param type
	 * @param payloads
	 * @return
	 */
	private Publisher<Payload> startStream(long streamId, FrameType type, Publisher<Payload> payloads) {
		if (payloads == null) {
			throw new IllegalStateException("Payloads can not be null");
		}
		if (type == null) {
			throw new IllegalStateException("FrameType can not be null");
		}
		return startStream(streamId, type, null, payloads);
	}

	/*
	 * Using payload/payloads with null check for efficiency so I don't have to allocate a Publisher for the most common case of single Payload
	 */
	private Publisher<Payload> startStream(long streamId, FrameType type, Payload payload, Publisher<Payload> payloads) {
		if (payload == null && payloads == null) {
			throw new IllegalStateException("Both payload and payloads can not be null");
		}

		return (Subscriber<? super Payload> child) -> {
			child.onSubscribe(new Subscription() {

				boolean started = false;
				StreamInputSubscriber streamInputSubscriber;
				UnicastSubject<Frame> writer;
				AtomicReference<Subscription> payloadsSubscription;

				@Override
				public void request(long n) {
					if (!started) {
						started = true;

						if (payloads != null) {
							payloadsSubscription = new AtomicReference<Subscription>();
						}

						// Response frames for this Stream
						UnicastSubject<Frame> transportInputSubject = UnicastSubject.create();
						streamInputMap.put(streamId, transportInputSubject);
						streamInputSubscriber = new StreamInputSubscriber(child, this::cancel);
						transportInputSubject.subscribe(streamInputSubscriber);

						// connect to transport
						writer = UnicastSubject.create(w -> {
							// does lease allow for sending request
							if (!canRequest())
							{
								w.onError(new LeaseException());
							}
							else
							{
								numberOfRemainingRequests--;
							}

							// when transport connects we write the request frame for this stream
							if (payload != null) {
								w.onNext(Frame.fromRequest(streamId, type, payload, n));
							} else {
								payloads.subscribe(new Subscriber<Payload>() {

									@Override
									public void onSubscribe(Subscription s) {
										if (!payloadsSubscription.compareAndSet(null, s)) {
											s.cancel(); // we are already unsubscribed
										}
										s.request(Long.MAX_VALUE); // TODO need REQUEST_N semantics from the other end
									}

									@Override
									public void onNext(Payload p) {
										w.onNext(Frame.fromRequest(streamId, type, p, n));
									}

									@Override
									public void onError(Throwable t) {
										// TODO validate with unit tests
										child.onError(new RuntimeException("Error received from request stream.", t));
										cancel();
									}

									@Override
									public void onComplete() {
										// do nothing if input ends, completion is handled by response side
									}

								});
							}

						});
						connection.addOutput(writer, new Completable()
						{

							@Override
							public void success()
							{
								// nothing to do onSuccess
							}

							@Override
							public void error(Throwable e) {
								child.onError(e);
								cancel();
							}

						});
					} else {
						// propagate further requestN frames
						writer.onNext(Frame.fromRequestN(streamId, n)); // TODO add N
					}

				}

				@Override
				public void cancel() {
					streamInputMap.remove(streamId);
					if (!streamInputSubscriber.terminated.get()) {
						writer.onNext(Frame.from(streamId, FrameType.CANCEL));
					}
					streamInputSubscriber.parentSubscription.cancel();
					if (payloadsSubscription != null) {
						if (!payloadsSubscription.compareAndSet(null, EmptySubscription.EMPTY)) {
							payloadsSubscription.get().cancel(); // unsubscribe it if it already exists
						}
					}
				}

			});
		};
	}

	private final static class StreamInputSubscriber implements Subscriber<Frame> {
		final AtomicBoolean terminated = new AtomicBoolean(false);
		Subscription parentSubscription;

		private final Subscriber<? super Payload> child;
		private final Runnable cancelAction;

		public StreamInputSubscriber(Subscriber<? super Payload> child, Runnable cancelAction) {
			this.child = child;
			this.cancelAction = cancelAction;
		}

		@Override
		public void onSubscribe(Subscription s) {
			this.parentSubscription = s;
			s.request(Long.MAX_VALUE); // no backpressure to transport (we will only receive what we've asked for already)
		}

		@Override
		public void onNext(Frame frame) {
			FrameType type = frame.getType();
			// convert ERROR messages into terminal events
			if (type == FrameType.NEXT) {
				child.onNext(frame);
			} else if (type == FrameType.COMPLETE) {
				terminated.set(true);
				onComplete();
				cancel();
			} else if (type == FrameType.NEXT_COMPLETE) {
				terminated.set(true);
				child.onNext(frame);
				onComplete();
				cancel();
			} else if (type == FrameType.ERROR) {
				terminated.set(true);
				final ByteBuffer byteBuffer = frame.getData();
				String errorMessage = getByteBufferAsString(byteBuffer);
				onError(new RuntimeException(errorMessage));
				cancel();
			} else {
				onError(new RuntimeException("Unexpected FrameType: " + frame.getType()));
				cancel();
			}
		}

		@Override
		public void onError(Throwable t) {
			terminated.set(true);
			child.onError(t);
		}

		@Override
		public void onComplete() {
			terminated.set(true);
			child.onComplete();
		}

		private void cancel() {
			cancelAction.run();
		}
	}

	private long nextStreamId() {
		return streamCount += 2; // go by two since server is odd, client is even
	}

	private void start() {
		AtomicReference<Subscription> connectionSubscription = new AtomicReference<>();
		// get input from responder->requestor for responses
		connection.getInput().subscribe(new Subscriber<Frame>() {
			public void onSubscribe(Subscription s) {
				if (connectionSubscription.compareAndSet(null, s)) {
					s.request(Long.MAX_VALUE);

					// now that we are connected, send SETUP frame (asynchronously, other messages can continue being written after this)
					connection.addOutput(PublisherUtils.just(Frame.fromSetup(setupPayload.getFlags(), 0, 0, setupPayload.metadataMimeType(), setupPayload.dataMimeType(), setupPayload)),
							new Completable() {

						@Override
						public void success() {
							// nothing to do
						}

						@Override
						public void error(Throwable e) {
							tearDown(e);
						}

					});
				} else {
					// means we already were cancelled
					s.cancel();
				}
			}

			private void tearDown(Throwable e) {
				onError(e);
			}

			public void onNext(Frame frame) {
				if (frame.getStreamId() == 0) {
					if (FrameType.SETUP_ERROR.equals(frame.getType())) {
						onError(new SetupException(frame));
					} else if (FrameType.LEASE.equals(frame.getType()) && honorLease) {
						numberOfRemainingRequests = Frame.Lease.numberOfRequests(frame);
						ttlExpiration = System.nanoTime() + Frame.Lease.ttl(frame);
					} else {
						onError(new RuntimeException("Received unexpected message type on stream 0: " + frame.getType().name()));
					}
				} else {
					UnicastSubject<Frame> streamSubject = streamInputMap.get(frame.getStreamId());
					if (streamSubject == null) {
						// if we can't find one, we have a problem with the overall connection and must tear down
						if (frame.getType() == FrameType.ERROR) {
							String errorMessage = getByteBufferAsString(frame.getData());
							onError(new RuntimeException("Received error for non-existent stream: " + frame.getStreamId() + " Message: " + errorMessage));
						} else {
							onError(new RuntimeException("Received message for non-existent stream: " + frame.getStreamId()));
						}
					} else {
						streamSubject.onNext(frame);
					}
				}
			}

			public void onError(Throwable t) {
				streamInputMap.forEach((id, subject) -> subject.onError(t));
				// TODO: iterate over responder side and destroy world
				errorStream.accept(t);
			}

			public void onComplete() {
				// TODO: might be a RuntimeException
				streamInputMap.forEach((id, subject) -> subject.onComplete());
			}

			public void cancel() { // TODO this isn't used ... is it supposed to be?
				if (!connectionSubscription.compareAndSet(null, CANCELLED)) {
					// cancel the one that was there if we failed to set the sentinel
					connectionSubscription.get().cancel();
				}
			}
		});
	}

	private static String getByteBufferAsString(ByteBuffer bb) {
		final byte[] bytes = new byte[bb.capacity()];
		bb.get(bytes);
		return new String(bytes, Charset.forName("UTF-8"));
	}

	private boolean canRequest()
	{
		boolean result = false;

		if (!honorLease)
		{
			result = true;
		}
		else if (numberOfRemainingRequests > 0 && (System.nanoTime() < ttlExpiration))
		{
			result = true;
		}

		return result;
	}
}
