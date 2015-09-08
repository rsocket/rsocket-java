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

import static io.reactivesocket.ConnectionSetupPayload.HONOR_LEASE;
import static io.reactivesocket.ConnectionSetupPayload.NO_FLAGS;
import static io.reactivesocket.TestUtil.*;
import static org.junit.Assert.*;
import static rx.Observable.*;
import static rx.RxReactiveStreams.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.*;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.reactivestreams.Publisher;

import rx.Subscription;
import rx.observables.ConnectableObservable;
import rx.observers.TestSubscriber;

@RunWith(Theories.class)
public class ReactiveSocketTest {

	private TestConnection clientConnection;
	private ReactiveSocket socketServer;
	private ReactiveSocket socketClient;
	private AtomicBoolean helloSubscriptionRunning = new AtomicBoolean(false);
	private AtomicReference<String> lastFireAndForget = new AtomicReference<String>();
	private AtomicReference<Throwable> lastServerError = new AtomicReference<Throwable>();
	private CountDownLatch lastServerErrorCountDown;
	private CountDownLatch fireAndForget;

	public static @DataPoints int[] setupFlags = {NO_FLAGS, HONOR_LEASE};

	@Before
	public void setup() {
		TestConnection serverConnection = new TestConnection();
		clientConnection = new TestConnection();
		clientConnection.connectToServerConnection(serverConnection);
		fireAndForget = new CountDownLatch(1);
		lastServerErrorCountDown = new CountDownLatch(1);

		socketServer = ReactiveSocket.fromServerConnection(serverConnection, setup -> new RequestHandler() {

			@Override
			public Publisher<Payload> handleRequestResponse(Payload payload) {
				String request = byteToString(payload.getData());
				if ("hello".equals(request)) {
					return toPublisher(just(utf8EncodedPayload("hello world", null)));
				} else {
					return toPublisher(error(new RuntimeException("Not Found")));
				}
			}

			@Override
			public Publisher<Payload> handleRequestStream(Payload payload) {
				String request = byteToString(payload.getData());
				if ("hello".equals(request)) {
					return toPublisher(range(0, 100).map(i -> "hello world " + i).map(n -> utf8EncodedPayload(n, null)));
				} else {
					return toPublisher(error(new RuntimeException("Not Found")));
				}
			}

			@Override
			public Publisher<Payload> handleSubscription(Payload payload) {
				String request = byteToString(payload.getData());
				if ("hello".equals(request)) {
					return toPublisher(interval(1, TimeUnit.MICROSECONDS)
						.doOnSubscribe(() -> helloSubscriptionRunning.set(true))
						.doOnUnsubscribe(() -> helloSubscriptionRunning.set(false))
						.map(i -> "subscription " + i)
						.map(n -> utf8EncodedPayload(n, null)));
				} else {
					return toPublisher(error(new RuntimeException("Not Found")));
				}
			}

			@Override
			public Publisher<Void> handleFireAndForget(Payload payload) {
				try {
					String request = byteToString(payload.getData());
					lastFireAndForget.set(request);
					if ("log".equals(request)) {
						return toPublisher(empty()); // success
					} else if ("blowup".equals(request)) {
						throw new RuntimeException("forced blowup to simulate handler error");
					} else {
						lastFireAndForget.set("notFound");
						return toPublisher(error(new RuntimeException("Not Found")));
					}
				} finally {
					fireAndForget.countDown();
				}
			}

			/**
			 * Use Payload.metadata for routing
			 */
			@Override
			public Publisher<Payload> handleChannel(Payload initialPayload, Publisher<Payload> payloads) {
				String request = byteToString(initialPayload.getMetadata());
				if ("echo".equals(request)) {
					return echoChannel(payloads);
				} else {
					return toPublisher(error(new RuntimeException("Not Found")));
				}
			}

			@Override
			public Publisher<Void> handleMetadataPush(Payload payload)
			{
				return toPublisher(error(new RuntimeException("Not Found")));
			}

			private Publisher<Payload> echoChannel(Publisher<Payload> echo) {
				return toPublisher(toObservable(echo).map(p -> {
					return utf8EncodedPayload(byteToString(p.getData()) + "_echo", null);
				}));
			}

		}, LeaseGovernor.UNLIMITED_LEASE_GOVERNOR, t -> {
			lastServerError.set(t);
			lastServerErrorCountDown.countDown();
		});
	}

	@After
	public void shutdown() {
		socketServer.shutdown();
		socketClient.shutdown();
	}

	private void startSockets(int setupFlag) {
		if (setupFlag == NO_FLAGS) {
			System.out.println("Reactivesocket configured with: NO_FLAGS");
		} else if (setupFlag == HONOR_LEASE) {
			System.out.println("Reactivesocket configured with: HONOR_LEASE");
		}
		socketClient = ReactiveSocket.fromClientConnection(
			clientConnection,
			ConnectionSetupPayload.create("UTF-8", "UTF-8", setupFlag),
			new FairLeaseGovernor(100, 5, TimeUnit.SECONDS)
		);

		// start both the server and client and monitor for errors
		//socketServer.start(LeaseGovernor.UNLIMITED_LEASE_GOVERNOR);
		socketServer.start();
		socketClient.start();

		awaitSocketAvailability(socketClient, 50, TimeUnit.SECONDS);
	}

	private void awaitSocketAvailability(ReactiveSocket socket, long timeout, TimeUnit unit) {
		long waitTimeMs = 1L;
		long startTime = System.nanoTime();
		long timeoutNanos = TimeUnit.NANOSECONDS.convert(timeout, unit);

		while (socket.availability() == 0.0) {
			try {
				Thread.sleep(waitTimeMs);
				waitTimeMs = Math.min(waitTimeMs * 2, 1000L);
				final long elapsedNanos = System.nanoTime() - startTime;
				if (elapsedNanos > timeoutNanos) {
					throw new IllegalStateException("Timeout while waiting for socket availability");
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		assertTrue("client socket has positive avaibility", socket.availability() > 0.0);
	}

	@Test
	@Theory
	public void testRequestResponse(int setupFlag) {
		startSockets(setupFlag);
		// perform request/response

		Publisher<Payload> response = socketClient.requestResponse(TestUtil.utf8EncodedPayload("hello", null));
		TestSubscriber<Payload> ts = TestSubscriber.create();
		toObservable(response).subscribe(ts);
		ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
		ts.assertNoErrors();
		ts.assertValue(TestUtil.utf8EncodedPayload("hello world", null));
	}

	@Test
	@Theory
	public void testRequestStream(int setupFlag) {
		startSockets(setupFlag);
		// perform request/stream

		Publisher<Payload> response = socketClient.requestStream(TestUtil.utf8EncodedPayload("hello", null));
		TestSubscriber<Payload> ts = TestSubscriber.create();
		toObservable(response).subscribe(ts);
		ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
		ts.assertNoErrors();
		assertEquals(100, ts.getOnNextEvents().size());
		assertEquals("hello world 99", byteToString(ts.getOnNextEvents().get(99).getData()));
	}

	@Test
	@Theory
	public void testRequestSubscription(int setupFlag) {
		startSockets(setupFlag);
		// perform request/subscription

		Publisher<Payload> response = socketClient.requestSubscription(TestUtil.utf8EncodedPayload("hello", null));
		TestSubscriber<Payload> ts = TestSubscriber.create();
		TestSubscriber<Payload> ts2 = TestSubscriber.create();
		ConnectableObservable<Payload> published = toObservable(response).publish();
		published.take(10).subscribe(ts);
		published.subscribe(ts2);
		Subscription subscription = published.connect();

		// ts completed due to take
		ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
		ts.assertNoErrors();
		ts.assertCompleted();

		// ts2 should never complete
		ts2.assertNoErrors();
		ts2.assertNoTerminalEvent();

		// assert it is running still
		assertTrue(helloSubscriptionRunning.get());

		// shut down the work
		subscription.unsubscribe();

		// wait for up to 2 seconds for the async CANCEL to occur (it sends a message up)
		for (int i = 0; i < 20; i++) {
			if (!helloSubscriptionRunning.get()) {
				break;
			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
			}
		}
		// and then stopped after unsubscribing
		assertFalse(helloSubscriptionRunning.get());

		assertEquals(10, ts.getOnNextEvents().size());
		assertEquals("subscription 9", byteToString(ts.getOnNextEvents().get(9).getData()));
	}

	@Test
	@Theory
	public void testFireAndForgetSuccess(int setupFlag) throws InterruptedException {
		startSockets(setupFlag);

		// perform request/response

		Publisher<Void> response = socketClient.fireAndForget(TestUtil.utf8EncodedPayload("log", null));
		TestSubscriber<Void> ts = TestSubscriber.create();
		toObservable(response).subscribe(ts);
		// these only test client side since this is fireAndForget
		ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
		ts.assertNoErrors();
		ts.assertCompleted();
		// this waits for server-side
		fireAndForget.await(500, TimeUnit.MILLISECONDS);
		assertEquals("log", lastFireAndForget.get());
	}

	@Test
	@Theory
	public void testFireAndForgetServerSideErrorNotFound(int setupFlag) throws InterruptedException {
		startSockets(setupFlag);
		// perform request/response

		Publisher<Void> response = socketClient.fireAndForget(TestUtil.utf8EncodedPayload("unknown", null));
		TestSubscriber<Void> ts = TestSubscriber.create();
		toObservable(response).subscribe(ts);
		// these only test client side since this is fireAndForget
		ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
		ts.assertNoErrors();// client-side won't see an error
		ts.assertCompleted();
		// this waits for server-side
		fireAndForget.await(500, TimeUnit.MILLISECONDS);
		assertEquals("notFound", lastFireAndForget.get());
	}

	@Test
	@Theory
	public void testFireAndForgetServerSideErrorHandlerBlowup(int setupFlag) throws InterruptedException {
		startSockets(setupFlag);
		// perform request/response

		Publisher<Void> response = socketClient.fireAndForget(TestUtil.utf8EncodedPayload("blowup", null));
		TestSubscriber<Void> ts = TestSubscriber.create();
		toObservable(response).subscribe(ts);
		// these only test client side since this is fireAndForget
		ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
		ts.assertNoErrors();// client-side won't see an error
		ts.assertCompleted();
		// this waits for server-side
		fireAndForget.await(500, TimeUnit.MILLISECONDS);
		assertEquals("blowup", lastFireAndForget.get());
		lastServerErrorCountDown.await(500, TimeUnit.MILLISECONDS);
		assertEquals("forced blowup to simulate handler error", lastServerError.get().getCause().getMessage());
	}

	@Test
	@Theory
	public void testRequestChannelEcho(int setupFlag) {
		startSockets(setupFlag);

		Publisher<Payload> requestStream = toPublisher(just(TestUtil.utf8EncodedPayload("1", "echo")).concatWith(just(TestUtil.utf8EncodedPayload("2", null))));
		Publisher<Payload> response = socketClient.requestChannel(requestStream);
		TestSubscriber<Payload> ts = TestSubscriber.create();
		toObservable(response).subscribe(ts);
		ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
		ts.assertNoErrors();
		assertEquals(2, ts.getOnNextEvents().size());
		assertEquals("1_echo", byteToString(ts.getOnNextEvents().get(0).getData()));
		assertEquals("2_echo", byteToString(ts.getOnNextEvents().get(1).getData()));
	}

	@Test
	@Theory
	public void testRequestChannelNotFound(int setupFlag) {
		startSockets(setupFlag);

		Publisher<Payload> requestStream = toPublisher(just(TestUtil.utf8EncodedPayload(null, "someChannel")));
		Publisher<Payload> response = socketClient.requestChannel(requestStream);
		TestSubscriber<Payload> ts = TestSubscriber.create();
		toObservable(response).subscribe(ts);
		ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
		ts.assertTerminalEvent();
		ts.assertNotCompleted();
		ts.assertNoValues();
		assertEquals("Not Found", ts.getOnErrorEvents().get(0).getMessage());
	}
}
