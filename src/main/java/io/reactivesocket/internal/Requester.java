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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import io.reactivesocket.exceptions.LeaseException;
import io.reactivesocket.exceptions.NotSentException;
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
	private final static long epoch = System.nanoTime();

	private final boolean isServer;
	private final DuplexConnection connection;
	private final Long2ObjectHashMap<UnicastSubject<Frame>> streamInputMap = new Long2ObjectHashMap<>();
	private final ConnectionSetupPayload setupPayload;
	private final Consumer<Throwable> errorStream;

	private final boolean honorLease;
	private long ttlExpiration;
	private long numberOfRemainingRequests = 0;
	private int streamCount = 0; // 0 is reserved for setup, all normal messages are >= 1

	private static final long DEFAULT_BATCH = 1024;
	private static final long REQUEST_THRESHOLD = 256;
	
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

		this.honorLease = setupPayload.willClientHonorLease();
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
	 * @param payload
	 * @return
	 */
	public Publisher<Payload> requestResponse(final Payload payload) {
		return startRequestResponse(nextStreamId(), FrameType.REQUEST_RESPONSE, payload);
	}

	/**
	 * Request/Stream with a finite multi-message response followed by a terminal state {@link Subscriber#onComplete()} or {@link Subscriber#onError(Throwable)}.
	 * 
	 * @param payload
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
	 * @param payload
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
							numberOfRemainingRequests--;

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
	 * @param payload
	 * @return
	 */
	public Publisher<Payload> requestSubscription(final Payload payload) {
		return startStream(nextStreamId(), FrameType.REQUEST_SUBSCRIPTION, payload);
	}

	/**
	 * Request/Stream with a finite multi-message response followed by a terminal state {@link Subscriber#onComplete()} or {@link Subscriber#onError(Throwable)}.
	 * 
	 * @param payloadStream
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
	public double availability() {
		if (!honorLease) {
			return 1.0;
		}
		final long now = System.currentTimeMillis();
		double available = 0.0;
		if (numberOfRemainingRequests > 0 && (now < ttlExpiration)) {
			available = 1.0;
		}
		return available;
	}

	/**
	 * Start a stream with a single request Payload.
	 * 
	 * @param streamId
	 * @param type
	 * @param payload
	 * @return
	 */
	private Publisher<Payload> startStream(int streamId, FrameType type, Payload payload) {
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
	private Publisher<Payload> startStream(int streamId, FrameType type, Publisher<Payload> payloads) {
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
	private Publisher<Payload> startStream(int streamId, FrameType type, Payload payload, Publisher<Payload> payloads) {
		if (payload == null && payloads == null) {
			throw new IllegalStateException("Both payload and payloads can not be null");
		}

		return (Subscriber<? super Payload> child) -> {
			child.onSubscribe(new Subscription() {

				boolean started = false;
				StreamInputSubscriber streamInputSubscriber;
				UnicastSubject<Frame> writer;
				AtomicReference<Subscription> payloadsSubscription;
				final AtomicLong requested = new AtomicLong(); // TODO does this need to be atomic? Can request(n) come from any thread?
				final AtomicLong outstanding = new AtomicLong(); // TODO AtomicLong just so I can pass it around ... perf issue? or is there a thread-safety issue?
				
				@Override
				public void request(long n) {
					// TODO are there concurrency issues we need to deal with here?
					BackpressureUtils.getAndAddRequest(requested, n);
					if (!started) {
						started = true;

						// determine initial RequestN
						long currentN = requested.get();
						final long requestN = currentN < DEFAULT_BATCH ? currentN : DEFAULT_BATCH;
						// threshold
						final long threshold = requestN == DEFAULT_BATCH ? REQUEST_THRESHOLD : requestN/3;

						if (payloads != null) {
							payloadsSubscription = new AtomicReference<>();
						}

						
						// declare output to transport
						writer = UnicastSubject.create((w, rn) -> {
							numberOfRemainingRequests--;

							// decrement as we request it
							requested.addAndGet(-requestN);
							// record how many we have requested
							outstanding.addAndGet(requestN);

							// when transport connects we write the request frame for this stream
							if (payload != null) {
								w.onNext(Frame.fromRequest(streamId, type, payload, (int) requestN));
							} else {
								// TODO hook this in via addOutput so requestN flows through correctly
//								connection.addOutput(payloads.map(p -> ... convert things to Frame here ...), new Completable()
//								{
//
//									@Override
//									public void success()
//									{
//										// nothing to do onSuccess
//									}
//
//									@Override
//									public void error(Throwable e) {
//										child.onError(e);
//										cancel();
//									}
//
//								});
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
										w.onNext(Frame.fromRequest(streamId, type, p, (int) requestN));
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

						// Response frames for this Stream
						UnicastSubject<Frame> transportInputSubject = UnicastSubject.create();
						synchronized(Requester.this) {
							streamInputMap.put(streamId, transportInputSubject);
						}
						streamInputSubscriber = new StreamInputSubscriber(streamId, threshold, outstanding, requested, writer, child, () -> {
							cancel();
						});
						transportInputSubject.subscribe(streamInputSubscriber);
						
						// connect to transport
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
								if (!(e instanceof NotSentException)) {
									cancel();
								}
							}

						});
					} else {
						// propagate further requestN frames
						long currentN = requested.get();
						final long requestThreshold = REQUEST_THRESHOLD < currentN ? REQUEST_THRESHOLD : currentN/3;
						requestIfNecessary(streamId, requestThreshold, currentN, outstanding.get(), writer, requested, outstanding);
					}
				}

				@Override
				public void cancel() {
					synchronized(Requester.this) {
						streamInputMap.remove(streamId);
					}
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
	
	/*
	 * Special-cased for performance reasons (achieved 20-30% throughput increase over using startStream for request/response)
	 */
	private Publisher<Payload> startRequestResponse(int streamId, FrameType type, Payload payload) {
		if (payload == null) {
			throw new IllegalStateException("Both payload and payloads can not be null");
		}

		return (Subscriber<? super Payload> child) -> {
			child.onSubscribe(new Subscription() {

				boolean started = false;
				StreamInputSubscriber streamInputSubscriber;
				UnicastSubject<Frame> writer;
				
				@Override
				public void request(long n) {
					if (!started) {
						started = true;
						// Response frames for this Stream
						UnicastSubject<Frame> transportInputSubject = UnicastSubject.create();
						synchronized(Requester.this) {
							streamInputMap.put(streamId, transportInputSubject);
						}
						streamInputSubscriber = new StreamInputSubscriber(streamId, 0, null, null, writer, child, () -> {
							cancel();
						});
						transportInputSubject.subscribe(streamInputSubscriber);
						
						Publisher<Frame> requestOutput = PublisherUtils.just(Frame.fromRequest(streamId, type, payload, 1));
						// connect to transport
						connection.addOutput(requestOutput, new Completable() {

							@Override
							public void success() {
								// nothing to do onSuccess
							}

							@Override
							public void error(Throwable e) {
								child.onError(e);
								cancel();
							}

						});
					}
				}

				@Override
				public void cancel() {
					if (!streamInputSubscriber.terminated.get()) {
						connection.addOutput(PublisherUtils.just(Frame.from(streamId, FrameType.CANCEL)), new Completable() {

							@Override
							public void success() {
								// nothing to do onSuccess
							}

							@Override
							public void error(Throwable e) {
								child.onError(e);
							}

						});
					}
					synchronized(Requester.this) {
						streamInputMap.remove(streamId);
					}
					streamInputSubscriber.parentSubscription.cancel();
				}

			});
		};
	}
	
	private final static class StreamInputSubscriber implements Subscriber<Frame> {
		final AtomicBoolean terminated = new AtomicBoolean(false);
		Subscription parentSubscription;

		private final int streamId;
		private final long requestThreshold;
		private final AtomicLong outstandingRequests;
		private final AtomicLong requested;
		private final UnicastSubject<Frame> writer;
		private final Subscriber<? super Payload> child;
		private final Runnable cancelAction;

		public StreamInputSubscriber(int streamId, long threshold, AtomicLong outstanding, AtomicLong requested, UnicastSubject<Frame> writer, Subscriber<? super Payload> child, Runnable cancelAction) {
			this.streamId = streamId;
			this.requestThreshold = threshold;
			this.requested = requested;
			this.outstandingRequests = outstanding;
			this.writer = writer;
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
			if (type == FrameType.NEXT_COMPLETE) {
				terminated.set(true);
				child.onNext(frame);
				onComplete();
				cancel();
			} else if (type == FrameType.NEXT) {
				child.onNext(frame);
				long currentOutstanding = outstandingRequests.decrementAndGet();
				requestIfNecessary(streamId, requestThreshold, requested.get(), currentOutstanding, writer, requested, outstandingRequests);
			} else if (type == FrameType.COMPLETE) {
				terminated.set(true);
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
	
	private static void requestIfNecessary(int streamId, long requestThreshold, long currentN, long currentOutstanding, UnicastSubject<Frame> writer, AtomicLong requested, AtomicLong outstanding) {
		if(currentOutstanding <= requestThreshold) {
			long batchSize = DEFAULT_BATCH - currentOutstanding;
			final long requestN = currentN < batchSize ? currentN : batchSize;
			
			if (requestN > 0) {
				// decrement as we request it
				requested.addAndGet(-requestN);
				// record how many we have requested
				outstanding.addAndGet(requestN);

				writer.onNext(Frame.fromRequestN(streamId, (int)requestN));
			}
		}
	}

	private int nextStreamId() {
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
				long streamId = frame.getStreamId();
				if (streamId == 0) {
					if (FrameType.SETUP_ERROR.equals(frame.getType())) {
						onError(new SetupException(frame));
					} else if (FrameType.LEASE.equals(frame.getType()) && honorLease) {
						numberOfRemainingRequests = Frame.Lease.numberOfRequests(frame);
						final long now = System.currentTimeMillis();
						final int ttl = Frame.Lease.ttl(frame);
						if (ttl == Integer.MAX_VALUE) {
							// Integer.MAX_VALUE represents infinity
							ttlExpiration = Long.MAX_VALUE;
						} else {
							ttlExpiration = now + ttl;
						}
					} else {
						onError(new RuntimeException("Received unexpected message type on stream 0: " + frame.getType().name()));
					}
				} else {
					UnicastSubject<Frame> streamSubject;
					synchronized(Requester.this) {
						streamSubject = streamInputMap.get(streamId);
					}
					if (streamSubject == null) {
						if (streamId <= streamCount) {
							// receiving a frame after a given stream has been cancelled/completed, so ignore (cancellation is async so there is a race condition)
							return; 
						} else {
							// message for stream that has never existed, we have a problem with the overall connection and must tear down
							if (frame.getType() == FrameType.ERROR) {
								String errorMessage = getByteBufferAsString(frame.getData());
								onError(new RuntimeException("Received error for non-existent stream: " + streamId + " Message: " + errorMessage));
							} else {
								onError(new RuntimeException("Received message for non-existent stream: " + streamId));
							}
						}
					} else {
						streamSubject.onNext(frame);
					}
				}
			}

			public void onError(Throwable t) {
				Collection<UnicastSubject<Frame>> subjects = null;
				synchronized(Requester.this) {
					subjects = streamInputMap.values();
				}
				subjects.forEach(subject -> subject.onError(t));
				// TODO: iterate over responder side and destroy world
				errorStream.accept(t);
				cancel();
			}

			public void onComplete() {
				Collection<UnicastSubject<Frame>> subjects = null;
				synchronized(Requester.this) {
					subjects = streamInputMap.values();
				}
				subjects.forEach(subject -> subject.onComplete());
				cancel();
			}

			public void cancel() { // TODO this isn't used ... is it supposed to be?
				if (!connectionSubscription.compareAndSet(null, CANCELLED)) {
					// cancel the one that was there if we failed to set the sentinel
					connectionSubscription.get().cancel();
					try {
						connection.close();
					} catch (IOException e) {
						errorStream.accept(e);
					}
				}
			}
		});
	}

	private static String getByteBufferAsString(ByteBuffer bb) {
		final byte[] bytes = new byte[bb.capacity()];
		bb.get(bytes);
		return new String(bytes, Charset.forName("UTF-8"));
	}
}
