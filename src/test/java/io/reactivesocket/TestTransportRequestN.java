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

import io.reactivesocket.lease.FairLeaseGovernor;
import io.reactivex.subscribers.TestSubscriber;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Publisher;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.reactivesocket.TestUtil.utf8EncodedPayload;
import static io.reactivex.Observable.error;
import static io.reactivex.Observable.fromPublisher;
import static io.reactivex.Observable.interval;
import static io.reactivex.Observable.just;
import static io.reactivex.Observable.range;
import static org.junit.Assert.fail;

/**
 * Ensure that request(n) from DuplexConnection "transport" layer is respected.
 *
 */
public class TestTransportRequestN {

	@Test(timeout = 3000)
	public void testRequestStreamWithNFromTransport() throws InterruptedException {
		clientConnection = new TestConnectionWithControlledRequestN();
		serverConnection = new TestConnectionWithControlledRequestN();
		setup(clientConnection, serverConnection);

		TestSubscriber<Payload> ts = new TestSubscriber<>();
		fromPublisher(socketClient.requestStream(utf8EncodedPayload("", null)))
				.take(150)
				.subscribe(ts);

		// wait for server to add output
		if (!serverConnection.awaitSubscription(1000)) {
			fail("Did not receive subscription");
		}
		// now request some data, but less than it is expected to output
		serverConnection.requestMore(10);

		// since we are async, give time for emission to occur
		Thread.sleep(500);

		// we should not have received more than 11 (10 + default 1 that is requested)

		if (ts.valueCount() > 11) {
			fail("Received more (" + ts.valueCount() + ") than transport requested (11)");
		}

		ts.cancel();

		// since we are async, give time for emission to occur
		Thread.sleep(500);

		if (serverConnection.emitted.get() > serverConnection.requested.get()) {
			fail("Emitted more (" + serverConnection.emitted.get() + ") than transport requested (" + serverConnection.requested.get() + ")");
		}
	}
	
	@Test(timeout = 3000)
	public void testRequestChannelDownstreamWithNFromTransport() throws InterruptedException {
		clientConnection = new TestConnectionWithControlledRequestN();
		serverConnection = new TestConnectionWithControlledRequestN();
		setup(clientConnection, serverConnection);

		TestSubscriber<Payload> ts = new TestSubscriber<>();
		fromPublisher(socketClient.requestChannel(just(utf8EncodedPayload("", null))))
				.take(150)
				.subscribe(ts);

		// wait for server to add output
		if (!serverConnection.awaitSubscription(1000)) {
			fail("Did not receive subscription");
		}
		// now request some data, but less than it is expected to output
		serverConnection.requestMore(10);

		// since we are async, give time for emission to occur
		Thread.sleep(500);

		// we should not have received more than 11 (10 + default 1 that is requested)

		if (ts.valueCount() > 11) {
			fail("Received more (" + ts.valueCount() + ") than transport requested (11)");
		}

		ts.cancel();

		// since we are async, give time for emission to occur
		Thread.sleep(500);

		if (serverConnection.emitted.get() > serverConnection.requested.get()) {
			fail("Emitted more (" + serverConnection.emitted.get() + ") than transport requested (" + serverConnection.requested.get() + ")");
		}
	}
	
	// TODO come back after some other work (Ben)
	@Ignore
	@Test(timeout = 3000)
	public void testRequestChannelUpstreamWithNFromTransport() throws InterruptedException {
		clientConnection = new TestConnectionWithControlledRequestN();
		serverConnection = new TestConnectionWithControlledRequestN();
		setup(clientConnection, serverConnection);

		TestSubscriber<Payload> ts = new TestSubscriber<>();
		fromPublisher(socketClient.requestChannel(range(0, 1000).map(i -> utf8EncodedPayload("" + i, null))))
				.take(10)
				.subscribe(ts);

		// wait for server to add output
		if (!serverConnection.awaitSubscription(1000)) {
			fail("Did not receive subscription");
		}
		// now request some data, but less than it is expected to output
		serverConnection.requestMore(10);
//		clientConnection.requestMore(2);

		// since we are async, give time for emission to occur
		Thread.sleep(500);

		// we should not have received more than 11 (10 + default 1 that is requested)

		if (ts.valueCount() > 11) {
			fail("Received more (" + ts.valueCount() + ") than transport requested (11)");
		}

		ts.cancel();

		// since we are async, give time for emission to occur
		Thread.sleep(500);

		if (serverConnection.emitted.get() > serverConnection.requested.get()) {
			fail("Server Emitted more (" + serverConnection.emitted.get() + ") than transport requested (" + serverConnection.requested.get() + ")");
		}
		
		if (clientConnection.emitted.get() > clientConnection.requested.get()) {
			fail("Client Emitted more (" + clientConnection.emitted.get() + ") than transport requested (" + clientConnection.requested.get() + ")");
		}
	}

	private TestConnectionWithControlledRequestN serverConnection;
	private TestConnectionWithControlledRequestN clientConnection;
	private ReactiveSocket socketServer;
	private ReactiveSocket socketClient;
	private AtomicBoolean helloSubscriptionRunning = new AtomicBoolean(false);
	private AtomicReference<Throwable> lastServerError = new AtomicReference<Throwable>();
	private CountDownLatch lastServerErrorCountDown;

	public void setup(TestConnectionWithControlledRequestN clientConnection, TestConnectionWithControlledRequestN serverConnection) throws InterruptedException {
		clientConnection.connectToServerConnection(serverConnection, false);
		lastServerErrorCountDown = new CountDownLatch(1);

		socketServer = ReactiveSocketImpl.fromServerConnection(serverConnection, setup -> new RequestHandler() {

			@Override
			public Publisher<Payload> handleRequestResponse(Payload payload) {
				return just(utf8EncodedPayload("request_response", null));
			}

			@Override
			public Publisher<Payload> handleRequestStream(Payload payload) {
				return range(0, 10000).map(i -> "stream_response_" + i).map(n -> utf8EncodedPayload(n, null));
			}

			@Override
			public Publisher<Payload> handleSubscription(Payload payload) {
				return interval(1, TimeUnit.MILLISECONDS)
						.onBackpressureDrop()
						.doOnSubscribe(s -> helloSubscriptionRunning.set(true))
						.doOnCancel(() -> helloSubscriptionRunning.set(false))
						.map(i -> "subscription " + i)
						.map(n -> utf8EncodedPayload(n, null));
			}

			@Override
			public Publisher<Void> handleFireAndForget(Payload payload) {
				return error(new RuntimeException("Not Found"));
			}

			/**
			 * Use Payload.metadata for routing
			 */
			@Override
			public Publisher<Payload> handleChannel(Payload initialPayload, Publisher<Payload> inputs) {
				return range(0, 10000).map(i -> "channel_response_" + i).map(n -> utf8EncodedPayload(n, null));
			}

			@Override
			public Publisher<Void> handleMetadataPush(Payload payload) {
				return error(new RuntimeException("Not Found"));
			}

		}, new FairLeaseGovernor(100, 10L, TimeUnit.SECONDS), t -> {
			t.printStackTrace();
			lastServerError.set(t);
			lastServerErrorCountDown.countDown();
		});

		socketClient = ReactiveSocketImpl.fromClientConnection(
				clientConnection,
				ConnectionSetupPayload.create("UTF-8", "UTF-8", ConnectionSetupPayload.NO_FLAGS),
				err -> err.printStackTrace());

		// start both the server and client and monitor for errors
		socketServer.startAndWait();
		socketClient.startAndWait();
	}

	@After
	public void shutdown() {
		socketServer.shutdown();
		socketClient.shutdown();
		try {
			clientConnection.close();
			serverConnection.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
