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
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Publisher;
import rx.RxReactiveStreams;
import rx.observers.TestSubscriber;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.reactivesocket.TestUtil.utf8EncodedPayload;
import static rx.Observable.*;
import static org.junit.Assert.fail;
import static rx.RxReactiveStreams.toPublisher;

/**
 * Ensure that request(n) from DuplexConnection "transport" layer is respected.
 *
 */
public class TransportRequestNTest {

	@Test(timeout = 3000)
	public void testRequestStreamWithNFromTransport() throws InterruptedException {
		clientConnection = new TestConnectionWithControlledRequestN();
		serverConnection = new TestConnectionWithControlledRequestN();
		setup(clientConnection, serverConnection);

		TestSubscriber<Payload> ts = new TestSubscriber<>();
		RxReactiveStreams.toObservable(socketClient.requestStream(utf8EncodedPayload("", null)))
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

		int valueCount = ts.getOnNextEvents().size();
		if (valueCount > 11) {
			fail("Received more (" + valueCount + ") than transport requested (11)");
		}

		ts.unsubscribe();

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
		RxReactiveStreams.toObservable(socketClient.requestStream(utf8EncodedPayload("", null)))
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
		int valueCount = ts.getOnNextEvents().size();
		if (valueCount > 11) {
			fail("Received more (" + valueCount + ") than transport requested (11)");
		}

		ts.unsubscribe();

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
		RxReactiveStreams.toObservable(socketClient.requestStream(utf8EncodedPayload("", null)))
						 .take(150)
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
		int valueCount = ts.getOnNextEvents().size();
		if (valueCount > 11) {
			fail("Received more (" + valueCount + ") than transport requested (11)");
		}

		ts.unsubscribe();

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
	private final AtomicBoolean helloSubscriptionRunning = new AtomicBoolean(false);
	private final AtomicReference<Throwable> lastServerError = new AtomicReference<>();
	private CountDownLatch lastServerErrorCountDown;

	public void setup(TestConnectionWithControlledRequestN clientConnection, TestConnectionWithControlledRequestN serverConnection) throws InterruptedException {
		clientConnection.connectToServerConnection(serverConnection, false);
		lastServerErrorCountDown = new CountDownLatch(1);

		socketServer = DefaultReactiveSocket.fromServerConnection(serverConnection, (setup,rs) -> new RequestHandler() {

			@Override
			public Publisher<Payload> handleRequestResponse(Payload payload) {
				return toPublisher(just(utf8EncodedPayload("request_response", null)));
			}

			@Override
			public Publisher<Payload> handleRequestStream(Payload payload) {
				return toPublisher(range(0, 10000).map(i -> "stream_response_" + i)
												  .map(n -> utf8EncodedPayload(n, null)));
			}

			@Override
			public Publisher<Payload> handleSubscription(Payload payload) {
				return toPublisher(interval(1, TimeUnit.MILLISECONDS)
						.onBackpressureDrop()
						.doOnSubscribe(() -> helloSubscriptionRunning.set(true))
						.doOnUnsubscribe(() -> helloSubscriptionRunning.set(false))
						.map(i -> "subscription " + i)
						.map(n -> utf8EncodedPayload(n, null)));
			}

			@Override
			public Publisher<Void> handleFireAndForget(Payload payload) {
				return toPublisher(error(new RuntimeException("Not Found")));
			}

			/**
			 * Use Payload.metadata for routing
			 */
			@Override
			public Publisher<Payload> handleChannel(Payload initialPayload, Publisher<Payload> inputs) {
				return toPublisher(range(0, 10000).map(i -> "channel_response_" + i)
									  .map(n -> utf8EncodedPayload(n, null)));
			}

			@Override
			public Publisher<Void> handleMetadataPush(Payload payload) {
				return toPublisher(error(new RuntimeException("Not Found")));
			}

		}, new FairLeaseGovernor(100, 10L, TimeUnit.SECONDS), t -> {
			t.printStackTrace();
			lastServerError.set(t);
			lastServerErrorCountDown.countDown();
		});

		socketClient = DefaultReactiveSocket.fromClientConnection(
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
