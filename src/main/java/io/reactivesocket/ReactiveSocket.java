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
public class ReactiveSocket implements AutoCloseable {
	private static final RequestHandler EMPTY_HANDLER = new RequestHandler.Builder().build();

	private static final Consumer<Throwable> DEFAULT_ERROR_STREAM = t -> {
		// TODO should we use SLF4j, use System.err, or swallow by default?
		System.err.println("ReactiveSocket ERROR => " + t.getMessage()
			+ " [Provide errorStream handler to replace this default]");
	};

	private final DuplexConnection connection;
	private final boolean isServer;
	private final Consumer<Throwable> errorStream;
	private Requester requester;
	private Responder responder;
	private final ConnectionSetupPayload requestorSetupPayload;
	private final ConnectionSetupHandler responderConnectionHandler;
	private final LeaseGovernor leaseGovernor;

	private ReactiveSocket(DuplexConnection connection, final boolean isServer, ConnectionSetupPayload requestorSetupPayload, final ConnectionSetupHandler responderConnectionHandler, LeaseGovernor leaseGovernor, Consumer<Throwable> errorStream) {
		this.connection = connection;
		this.isServer = isServer;
		this.requestorSetupPayload = requestorSetupPayload;
		this.responderConnectionHandler = responderConnectionHandler;
		this.leaseGovernor = leaseGovernor;
		this.errorStream = errorStream;
	}

	/**
	 * Create a ReactiveSocket from a client-side {@link DuplexConnection}. 
	 * <p>
	 * A client-side connection is one that initiated the connection with a server and will 
	 * define the ReactiveSocket behaviors via the {@link ConnectionSetupPayload} that define mime-types, 
	 * leasing behavior and other connection-level details.
	 * 
	 * @param connection
	 *            DuplexConnection of client-side initiated connection for the ReactiveSocket protocol to use.
	 * @param setup
	 *            ConnectionSetupPayload that defines mime-types and other connection behavior details.
	 * @param handler
	 *            (Optional) RequestHandler for responding to requests from the server. If 'null' requests will be responded to with "Not Found" errors.
	 * @param errorStream
	 *            (Optional) Callback for errors while processing streams over connection. If 'null' then error messages will be output to System.err.
	 * @return ReactiveSocket for start, shutdown and sending requests.
	 */
	public static ReactiveSocket fromClientConnection(DuplexConnection connection, ConnectionSetupPayload setup, RequestHandler handler, LeaseGovernor leaseGovernor, Consumer<Throwable> errorStream) {
		if(connection == null) {
			throw new IllegalArgumentException("DuplexConnection can not be null");
		}
		if(setup == null) {
			throw new IllegalArgumentException("ConnectionSetupPayload can not be null");
		}
		final RequestHandler h = handler != null ? handler : EMPTY_HANDLER;
		Consumer<Throwable> es = errorStream != null ? errorStream : DEFAULT_ERROR_STREAM;
		return new ReactiveSocket(connection, false, setup, s -> h, leaseGovernor, es);
	}

	/**
	 * Create a ReactiveSocket from a client-side {@link DuplexConnection}. 
	 * <p>
	 * A client-side connection is one that initiated the connection with a server and will 
	 * define the ReactiveSocket behaviors via the {@link ConnectionSetupPayload} that define mime-types, 
	 * leasing behavior and other connection-level details.
	 * <p>
	 * If this ReactiveSocket receives requests from the server it will respond with "Not Found" errors.
	 * 
	 * @param connection
	 *            DuplexConnection of client-side initiated connection for the ReactiveSocket protocol to use.
	 * @param setup
	 *            ConnectionSetupPayload that defines mime-types and other connection behavior details.
	 * @param errorStream
	 *            (Optional) Callback for errors while processing streams over connection. If 'null' then error messages will be output to System.err.
	 * @return ReactiveSocket for start, shutdown and sending requests.
	 */
	public static ReactiveSocket fromClientConnection(DuplexConnection connection, ConnectionSetupPayload setup, Consumer<Throwable> errorStream) {
		return fromClientConnection(connection, setup, EMPTY_HANDLER, LeaseGovernor.NULL_LEASE_GOVERNOR, errorStream);
	}

	public static ReactiveSocket fromClientConnection(DuplexConnection connection, ConnectionSetupPayload setup, LeaseGovernor leaseGovernor) {
		return fromClientConnection(connection, setup, EMPTY_HANDLER, leaseGovernor, DEFAULT_ERROR_STREAM);
	}

	/**
	 * Create a ReactiveSocket from a server-side {@link DuplexConnection}. 
	 * <p>
	 * A server-side connection is one that accepted the connection from a client and will 
	 * define the ReactiveSocket behaviors via the {@link ConnectionSetupPayload} that define mime-types, 
	 * leasing behavior and other connection-level details.
	 * 
	 * @param connection
	 * @param connectionHandler
	 * @param errorConsumer
	 * @return
	 */
	public static ReactiveSocket fromServerConnection(DuplexConnection connection, ConnectionSetupHandler connectionHandler, LeaseGovernor leaseGovernor, Consumer<Throwable> errorConsumer) {
		return new ReactiveSocket(connection, true, null, connectionHandler, leaseGovernor, errorConsumer);
	}

	public static ReactiveSocket fromServerConnection(DuplexConnection connection, ConnectionSetupHandler connectionHandler) {
		return fromServerConnection(connection, connectionHandler, LeaseGovernor.NULL_LEASE_GOVERNOR ,t -> {});
	}

	/**
	 * Initiate a request response exchange
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
			throw new IllegalStateException("Connection not initialized. Please 'start()' before submitting requests");
		}
	}

	/**
	 * Client check for availability to send request based on lease
	 *
	 * @return 0.0 to 1.0 indicating availability of sending requests
	 */
	public double availability()
	{
		// TODO: can happen in either direction
		assertRequester();
		return requester.availability();
	}

	/**
	 * Server granting new lease information to client
	 *
	 * Initial lease semantics are that server waits for periodic granting of leases by server side.
	 *
	 * @param ttl
	 * @param numberOfRequests
	 */
	public void sendLease(int ttl, int numberOfRequests)
	{
		// TODO: can happen in either direction
		responder.sendLease(ttl, numberOfRequests);
	}

	/**
	 * Start protocol processing on the given DuplexConnection.
	 */
	public final void start() {
		if (isServer) {
			responder = Responder.create(connection, responderConnectionHandler, leaseGovernor, errorStream);
			// requester = Requester.createServerRequester(connection);// TODO commented out until odd/even message routing is
		} else {
			requester = Requester.createClientRequester(connection, requestorSetupPayload, errorStream);
		}
	}

	@Override
	public void close() throws Exception {
		connection.close();
	}
	
	public void shutdown() {
		try {
			close();
			leaseGovernor.unregister(responder);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	private static <T> Publisher<T> error(Throwable e) {
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
