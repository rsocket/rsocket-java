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

import java.util.function.Consumer;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivesocket.internal.Requester;
import io.reactivesocket.internal.Responder;

/**
 * Interface for a connection that supports sending requests and receiving responses
 *
 * Created by servers for connections Created on demand for clients
 */
public class ReactiveSocket {
	private static final Publisher<Payload> NOT_FOUND_ERROR_PAYLOAD = error(new Exception("Not Found!"));
	private static final Publisher<Void> NOT_FOUND_ERROR_VOID = error(new Exception("Not Found!"));
	private static final RequestHandler EMPTY_HANDLER = new RequestHandler() {
		public Publisher<Payload> handleRequestResponse(Payload payload) {
			return NOT_FOUND_ERROR_PAYLOAD;
		}

		public Publisher<Payload> handleRequestStream(Payload payload) {
			return NOT_FOUND_ERROR_PAYLOAD;
		}

		public Publisher<Payload> handleSubscription(Payload payload) {
			return NOT_FOUND_ERROR_PAYLOAD;
		}

		public Publisher<Void> handleFireAndForget(Payload payload) {
			return NOT_FOUND_ERROR_VOID;
		}
		
		public Publisher<Payload> handleChannel(Payload initialPayload, Publisher<Payload> payloads) {
			return NOT_FOUND_ERROR_PAYLOAD;
		}
	};

	private final boolean isServer; // TODO get rid of this?
	private Requester requester; // can't initialized until connection is accepted
	private final Responder responder;
	private final ConnectionSetupPayload requestorSetupPayload;

	private ReactiveSocket(final boolean isServer, ConnectionSetupPayload requestorSetupPayload, final ConnectionSetup responderConnectionHandler, Consumer<Throwable> errorStream) {
		this.isServer = isServer;
		this.requestorSetupPayload = requestorSetupPayload;
		this.responder = Responder.create(responderConnectionHandler, errorStream);
	}
	
	public static ReactiveSocket createRequestor(ConnectionSetupPayload setup) {
		return new ReactiveSocket(false, setup, s -> EMPTY_HANDLER, t -> {});
	}
	
	public static ReactiveSocket createRequestor(String metadataMimeType, String dataMimeType) {
		return createRequestor(ConnectionSetupPayload.create(metadataMimeType, dataMimeType));
	}
	
	public static ReactiveSocket createRequestor(String metadataMimeType, String dataMimeType, Payload payload) {
		return createRequestor(ConnectionSetupPayload.create(metadataMimeType, dataMimeType, payload));
	}

	// TODO what name makes sense for these 'create' methods?
	// TODO what if someone wants to create just a Responder? This class has the 'request' methods on it.

	/**
	 * Create a ReactiveSocket and prepare for operation as a server
	 *
	 * @param connection
	 * @param requestHandler
	 * @param responderErrorConsumer Callback for all errors seen while processing requests in the Responder.
	 * @return
	 */
	public static ReactiveSocket createResponderAndRequestor(ConnectionSetup connectionHandler, Consumer<Throwable> errorConsumer) {
		final ReactiveSocket socket = new ReactiveSocket(true, null, connectionHandler, errorConsumer); // TODO broken with null SetupPayload

		// TODO: passively wait for a SETUP and accept or reject it

		return socket;
	}
	
	public static ReactiveSocket createResponderAndRequestor(ConnectionSetup connectionHandler) {
		return createResponderAndRequestor(connectionHandler, t -> {});
	}

	/**
	 * Initiate a request response exchange
	 *
	 * @param data
	 * @param metadata
	 * @return
	 */
	public Publisher<Payload> requestResponse(final Payload payload) {
		assertRequester();
		return requester.requestResponse(payload);
	}

	public Publisher<Void> fireAndForget(final Payload payload) {
		assertRequester();
		return requester.fireAndForget(payload);
	}

	public Publisher<Payload> requestStream(final Payload payload) {
		assertRequester();
		return requester.requestStream(payload);
	}

	public Publisher<Payload> requestSubscription(final Payload payload) {
		assertRequester();
		return requester.requestSubscription(payload);
	}
	
	public Publisher<Payload> requestChannel(final Publisher<Payload> payloads) {
		assertRequester();
		return requester.requestChannel(payloads);
	}

	private void assertRequester() {
		if (requester == null) {
			throw new IllegalStateException("Connection not initialized. Please 'connection' before submitting requests");
		}
	}

	/**
	 * Connect this ReactiveSocket with the given DuplexConnection.
	 * <p>
	 * NOTE: You must subscribe to the returned Publisher for anything to start.
	 * 
	 * @param connection
	 * @return
	 */
	public Publisher<Void> connect(DuplexConnection connection) {
		// TODO should we make this eager instead of lazy so people don't have to subscribe to the publisher if they want to ignore errors? how should errors then be handled?

		return new Publisher<Void>() {

			@Override
			public void subscribe(Subscriber<? super Void> child) {
				child.onSubscribe(new Subscription() {

					boolean started = false;

					@Override
					public void request(long n) {
						if (!started) {
							started = true;

							// connect the Requestor
							Publisher<Void> requesterConnectionHandler = null; // temporary until fix birectional
							// connect the Responder
							Publisher<Void> responderConnectionHandler = null; // temporary until fix birectional

							if (isServer) {
								responderConnectionHandler = responder.acceptConnection(connection);
								// requester = Requester.createServerRequester(connection);// TODO commented out until odd/even message routing is done
							} else {
								requester = Requester.createClientRequester(connection, requestorSetupPayload);
								requesterConnectionHandler = requester.start();
							}

							if (requesterConnectionHandler != null) {
								requesterConnectionHandler.subscribe(new Subscriber<Void>() {

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
										t.printStackTrace();
									}

									@Override
									public void onComplete() {
										child.onComplete(); // TODO need merge to wait for both
									}

								});
							}

							if (responderConnectionHandler != null) {
								responderConnectionHandler.subscribe(new Subscriber<Void>() {

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
										child.onError(t);
									}

									@Override
									public void onComplete() {
										child.onComplete(); // TODO need merge to wait for both
									}

								});
							}
						}
					}

					@Override
					public void cancel() {
						// TODO need to allow cancelling
					}

				});
			}

		};
	}

	private static final <T> Publisher<T> error(Throwable e) {
		return (Subscriber<? super T> s) -> {
			s.onSubscribe(new Subscription() {

				@Override
				public void request(long n) {
					// should probably worry about n==0
					s.onError(e);
				}

				@Override
				public void cancel() {
					// ignoring just because
				}

			});

		};
	}
}
