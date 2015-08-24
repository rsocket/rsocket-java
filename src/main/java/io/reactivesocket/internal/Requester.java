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
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Protocol implementation abstracted over a {@link DuplexConnection}.
 * <p>
 * Concrete implementations of {@link DuplexConnection} over TCP, WebSockets, Aeron, etc can be passed to this class for protocol handling.
 */
public class Requester {

	private final static Subscription CANCELLED = new EmptySubscription();

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
	 * @param isServer
	 *            for debugging purposes
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
	 * Request/Stream with a finite multi-message response followed by a terminal state {@link Subscriber#onComplete()} or {@link Subscriber#onError(Throwable)}.
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
	 * The returned {@link Publisher} will emit {@link Subscriber#onComplete()} or {@link Subscriber#onError(Throwable)} to represent success or failure in sending from the client side, but no
	 * feedback from the server will be returned.
	 * 
	 * @param data
	 * @return
	 */
	public Publisher<Void> fireAndForget(final Payload payload) {
		return connection.addOutput(PublisherUtils.just(Frame.from(nextStreamId(), FrameType.FIRE_AND_FORGET, payload)));
	}

	/**
	 * Event subscription with an infinite multi-message response potentially terminated with an {@link Subscriber#onError(Throwable)}.
	 * 
	 * @param data
	 * @return
	 */
	public Publisher<Payload> requestSubscription(final Payload payload) {
		return startStream(Frame.from(nextStreamId(), FrameType.REQUEST_SUBSCRIPTION, payload));
	}

	private Publisher<Payload> startStream(Frame requestFrame) {
		return (Subscriber<? super Payload> child) -> {
			child.onSubscribe(new Subscription() {

				boolean started = false;
				StreamInputSubscriber streamInputSubscriber;
				UnicastSubject writer;

				@Override
				public void request(long n) {
					if (!started) {
						started = true;

						// Response frames for this Stream
						UnicastSubject transportInputSubject = UnicastSubject.create();
						streamInputMap.put(requestFrame.getStreamId(), transportInputSubject);
						streamInputSubscriber = new StreamInputSubscriber(child, () -> {
							cancel();
						});
						transportInputSubject.subscribe(streamInputSubscriber);

						// connect to transport
						writer = UnicastSubject.create(w -> {
							// when transport connects we write the request frame for this stream
							w.onNext(requestFrame);
							// w.onNext(Frame.from(requestFrame.getStreamId(), FrameType.REQUEST_N)); // TODO add N
						});
						Publisher<Void> writeOutcome = connection.addOutput(writer); // TODO do something with this
						writeOutcome.subscribe(new Subscriber<Void>() {

							@Override
							public void onSubscribe(Subscription s) {
								s.request(Long.MAX_VALUE);
							}

							@Override
							public void onNext(Void t) {
							}

							@Override
							public void onError(Throwable t) {
								child.onError(t);
								cancel();
							}

							@Override
							public void onComplete() {
							}

						});
					} else {
						// propagate further requestN frames
						writer.onNext(Frame.from(requestFrame.getStreamId(), FrameType.REQUEST_N)); // TODO add N
					}

				}

				@Override
				public void cancel() {
					streamInputMap.remove(requestFrame.getStreamId());
					if (!streamInputSubscriber.terminated.get()) {
						writer.onNext(Frame.from(requestFrame.getStreamId(), FrameType.CANCEL));
					}
					streamInputSubscriber.parentSubscription.cancel();
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
			// convert ERROR messages into terminal events
			if (frame.getType() == FrameType.NEXT) {
				child.onNext(frame);
			} else if (frame.getType() == FrameType.COMPLETE) {
				terminated.set(true);
				onComplete();
				cancel();
			} else if (frame.getType() == FrameType.NEXT_COMPLETE) {
				terminated.set(true);
				child.onNext(frame);
				onComplete();
				cancel();
			} else if (frame.getType() == FrameType.ERROR) {
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

	private int nextStreamId() {
		// use ++ prefix so streamCount always equals the last stream created
		return ++streamCount;
	}

	public Publisher<Void> start() {
		return (Subscriber<? super Void> terminalObserver) -> {
			terminalObserver.onSubscribe(new Subscription() {

				boolean started = false;
				AtomicReference<Subscription> connectionSubscription = new AtomicReference<>();

				@Override
				public void request(long n) {
					if (!started) {
						started = true;
						// get input from responder->requestor for responses
						connection.getInput().subscribe(new Subscriber<Frame>() {
							public void onSubscribe(Subscription s) {
								if (connectionSubscription.compareAndSet(null, s)) {
									s.request(Long.MAX_VALUE);
								} else {
									// means we already were cancelled
									s.cancel();
								}
							}

							public void onNext(Frame frame) {
								UnicastSubject streamSubject = streamInputMap.get(frame.getStreamId());
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

							public void onError(Throwable t) {
								streamInputMap.forEach((id, subject) -> subject.onError(t));
								// TODO: iterate over responder side and destroy world
								terminalObserver.onError(t);
							}

							public void onComplete() {
								// TODO: might be a RuntimeException
								streamInputMap.forEach((id, subject) -> subject.onComplete());
								// TODO: iterate over responder side and destroy world
								terminalObserver.onComplete();
							}
						});
					}
				}

				@Override
				public void cancel() {
					if (!connectionSubscription.compareAndSet(null, CANCELLED)) {
						// cancel the one that was there if we failed to set the sentinel
						connectionSubscription.get().cancel();
					}
				}
			});
		};
	}

	private static String getByteBufferAsString(ByteBuffer bb) {
		final byte[] bytes = new byte[bb.capacity()];
		bb.get(bytes);
		return new String(bytes, Charset.forName("UTF-8"));
	}
}
