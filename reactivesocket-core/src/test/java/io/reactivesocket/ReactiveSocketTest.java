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
import io.reactivesocket.rx.Completable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Cancellation;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.core.test.TestSubscriber;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.reactivesocket.ConnectionSetupPayload.HONOR_LEASE;
import static io.reactivesocket.ConnectionSetupPayload.NO_FLAGS;
import static io.reactivesocket.TestUtil.byteToString;
import static io.reactivesocket.TestUtil.utf8EncodedPayload;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Theories.class)
public class ReactiveSocketTest {

	private TestConnection clientConnection;
	private ReactiveSocket socketServer;
	private ReactiveSocket socketClient;
	private AtomicBoolean helloSubscriptionRunning = new AtomicBoolean(false);
	private AtomicReference<String> lastFireAndForget = new AtomicReference<>();
	private AtomicReference<String> lastMetadataPush = new AtomicReference<>();
	private AtomicReference<Throwable> lastServerError = new AtomicReference<>();
	private CountDownLatch lastServerErrorCountDown;
	private CountDownLatch fireAndForgetOrMetadataPush;

	public static final @DataPoints int[] setupFlags = {NO_FLAGS, HONOR_LEASE};

	@Before
	public void setup() {
		TestConnection serverConnection = new TestConnection();
		clientConnection = new TestConnection();
		clientConnection.connectToServerConnection(serverConnection);
		fireAndForgetOrMetadataPush = new CountDownLatch(1);
		lastServerErrorCountDown = new CountDownLatch(1);

		socketServer = DefaultReactiveSocket.fromServerConnection(serverConnection, (setup,rs) -> new RequestHandler() {

			@Override
			public Publisher<Payload> handleRequestResponse(Payload payload) {
				String request = byteToString(payload.getData());
				System.out.println("********************************************************************************************** requestResponse: " + request);
				if ("hello".equals(request)) {
					System.out.println("********************************************************************************************** respond hello");
					return Flux.just(utf8EncodedPayload("hello world", null));
				} else {
					return Flux.error(new RuntimeException("Not Found"));
				}
			}

			@Override
			public Publisher<Payload> handleRequestStream(Payload payload) {
				String request = byteToString(payload.getData());
				if ("hello".equals(request)) {
					return Flux.range(0, 100).map(i -> "hello world " + i).map(n -> utf8EncodedPayload(n, null));
				} else {
					return Flux.error(new RuntimeException("Not Found"));
				}
			}

			@Override
			public Publisher<Payload> handleSubscription(Payload payload) {
				String request = byteToString(payload.getData());
				if ("hello".equals(request)) {
					return Flux.interval(1, Schedulers.newTimer("timer", 1))
						.onBackpressureDrop()
						.doOnSubscribe(s -> helloSubscriptionRunning.set(true))
						.doOnCancel(() -> helloSubscriptionRunning.set(false))
						.map(i -> "subscription " + i)
						.map(n -> utf8EncodedPayload(n, null));
				} else {
					return Flux.error(new RuntimeException("Not Found"));
				}
			}

			@Override
			public Publisher<Void> handleFireAndForget(Payload payload) {
				try {
					String request = byteToString(payload.getData());
					lastFireAndForget.set(request);
					if ("log".equals(request)) {
						return Flux.empty(); // success
					} else if ("blowup".equals(request)) {
						throw new RuntimeException("forced blowup to simulate handler error");
					} else {
						lastFireAndForget.set("notFound");
						return Flux.error(new RuntimeException("Not Found"));
					}
				} finally {
					fireAndForgetOrMetadataPush.countDown();
				}
			}

			/**
			 * Use Payload.metadata for routing
			 */
			@Override
			public Publisher<Payload> handleChannel(Payload initialPayload, Publisher<Payload> inputs) {
				return new Publisher<Payload>() {
					@Override
					public void subscribe(Subscriber<? super Payload> subscriber) {
						inputs.subscribe(new Subscriber<Payload>() {
							@Override
							public void onSubscribe(Subscription s) {
								subscriber.onSubscribe(s);
							}

							@Override
							public void onNext(Payload input) {
								String metadata = byteToString(input.getMetadata());
								String data = byteToString(input.getData());
								if ("echo".equals(metadata)) {
									subscriber.onNext(utf8EncodedPayload(data + "_echo", null));
								} else {
									onError(new RuntimeException("Not Found"));
								}
							}

							@Override
							public void onError(Throwable t) {
								subscriber.onError(t);
							}

							@Override
							public void onComplete() {
								subscriber.onComplete();
							}
						});
					}
				};
			}

			@Override
			public Publisher<Void> handleMetadataPush(Payload payload)
			{
				try {
					String request = byteToString(payload.getMetadata());
					lastMetadataPush.set(request);
					if ("log".equals(request)) {
						return Flux.empty(); // success
					} else if ("blowup".equals(request)) {
						throw new RuntimeException("forced blowup to simulate handler error");
					} else {
						lastMetadataPush.set("notFound");
						return Flux.error(new RuntimeException("Not Found"));
					}
				} finally {
					fireAndForgetOrMetadataPush.countDown();
				}
			}

			private Publisher<Payload> echoChannel(Publisher<Payload> echo) {
				return Flux.from(echo).map(p -> {
					return utf8EncodedPayload(byteToString(p.getData()) + "_echo", null);
				});
			}

//		}, LeaseGovernor.UNLIMITED_LEASE_GOVERNOR, t -> {
		}, new FairLeaseGovernor(100, 10L, TimeUnit.SECONDS), t -> {
			t.printStackTrace();
			lastServerError.set(t);
			lastServerErrorCountDown.countDown();
		});
	}

	@After
	public void shutdown() {
		socketServer.shutdown();
		socketClient.shutdown();
	}

	private void startSockets(int setupFlag, RequestHandler handler) throws InterruptedException {
		if (setupFlag == NO_FLAGS) {
			System.out.println("Reactivesocket configured with: NO_FLAGS");
		} else if (setupFlag == HONOR_LEASE) {
			System.out.println("Reactivesocket configured with: HONOR_LEASE");
		}
		socketClient = DefaultReactiveSocket.fromClientConnection(
			clientConnection,
			ConnectionSetupPayload.create("UTF-8", "UTF-8", setupFlag),
			handler, 
			err -> err.printStackTrace()
		);

		// start both the server and client and monitor for errors
        LatchedCompletable lc = new LatchedCompletable(2);
        socketServer.start(lc);
        socketClient.start(lc);
        if(!lc.await(3000, TimeUnit.MILLISECONDS)) {
        	throw new RuntimeException("Timed out waiting for startup");
        }

		awaitSocketAvailability(socketClient, 50, TimeUnit.SECONDS);
	}
	
	private void startSockets(int setupFlag) throws InterruptedException {
		startSockets(setupFlag, null);
	}

	private void awaitSocketAvailability(ReactiveSocket socket, long timeout, TimeUnit unit) {
		long waitTimeMs = 1L;
		long startTime = System.nanoTime();
		long timeoutNanos = TimeUnit.NANOSECONDS.convert(timeout, unit);

		while (socket.availability() == 0.0) {
			try {
				System.out.println("... waiting " + waitTimeMs + " ...");
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

	@Test(timeout = 2000)
	public void testShutdownListener() throws Exception {
		socketClient = DefaultReactiveSocket.fromClientConnection(
			clientConnection,
			ConnectionSetupPayload.create("UTF-8", "UTF-8", NO_FLAGS),
			err -> err.printStackTrace()
		);

		CountDownLatch latch = new CountDownLatch(1);

		socketClient.onShutdown(new Completable() {
			@Override
			public void success() {
				latch.countDown();
			}

			@Override
			public void error(Throwable e) {

			}
		});

		socketClient.close();

		latch.await();
	}

	@Test(timeout = 2000)
	public void testMultipleShutdownListeners() throws Exception {
		socketClient = DefaultReactiveSocket.fromClientConnection(
			clientConnection,
			ConnectionSetupPayload.create("UTF-8", "UTF-8", NO_FLAGS),
			err -> err.printStackTrace()
		);

		CountDownLatch latch = new CountDownLatch(2);

		socketClient
			.onShutdown(new Completable() {
				@Override
				public void success() {
					latch.countDown();
				}

				@Override
				public void error(Throwable e) {

				}
			});

		socketClient
			.onShutdown(new Completable() {
				@Override
				public void success() {
					latch.countDown();
				}

				@Override
				public void error(Throwable e) {

				}
			});

		socketClient.close();

		latch.await();
	}

	@Test(timeout=2000)
	@Theory
	public void testRequestResponse(int setupFlag) throws InterruptedException {
		startSockets(setupFlag);
		// perform request/response

		Publisher<Payload> response = socketClient.requestResponse(TestUtil.utf8EncodedPayload("hello", null));
		TestSubscriber<Payload> ts = TestSubscriber.create();
		response.subscribe(ts);
		ts.await();
		ts.assertNoError();
		ts.assertValuesWith(value ->
			assertEquals(TestUtil.utf8EncodedPayload("hello world", null), value)
		);
	}

	@Test(timeout=2000, expected=IllegalStateException.class)
	public void testRequestResponsePremature() throws InterruptedException {
		socketClient = DefaultReactiveSocket.fromClientConnection(
				clientConnection,
				ConnectionSetupPayload.create("UTF-8", "UTF-8", NO_FLAGS),
				err -> err.printStackTrace()
			);
		
		Publisher<Payload> response = socketClient.requestResponse(TestUtil.utf8EncodedPayload("hello", null));
	}

	@Test(timeout=2000)
	@Theory
	public void testRequestStream(int setupFlag) throws InterruptedException {
		startSockets(setupFlag);
		// perform request/stream

		Publisher<Payload> response = socketClient.requestStream(TestUtil.utf8EncodedPayload("hello", null));
		TestSubscriber<Payload> ts = TestSubscriber.create();
		response.subscribe(ts);
		ts.await();
		ts.assertNoError();
		ts.assertValueCount(100);
	}

	@Test(timeout=4000)
	@Theory
	public void testRequestSubscription(int setupFlag) throws InterruptedException {
		startSockets(setupFlag);
		// perform request/subscription

		Publisher<Payload> response = socketClient.requestSubscription(TestUtil.utf8EncodedPayload("hello", null));
		TestSubscriber<Payload> ts = TestSubscriber.create();
		TestSubscriber<Payload> ts2 = TestSubscriber.create();
		ConnectableFlux<Payload> published = ConnectableFlux.from(response).publish();
		published.take(10).subscribe(ts);
		published.subscribe(ts2);
		Cancellation subscription = published.connect();

		// ts completed due to take
		ts.await();
		ts.assertNoError();
		ts.assertComplete();

		// ts2 should never complete
		ts2.assertNoError();
		ts2.assertNotTerminated();

		// assert it is running still
		assertTrue(helloSubscriptionRunning.get());
		
		// shut down the work
		subscription.dispose();

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
		ts.assertValueCount(10);
	}

	@Test(timeout=2000)
	@Theory
	public void testFireAndForgetSuccess(int setupFlag) throws InterruptedException {
		startSockets(setupFlag);

		// perform request/response

		Publisher<Void> response = socketClient.fireAndForget(TestUtil.utf8EncodedPayload("log", null));
		TestSubscriber<Void> ts = TestSubscriber.create();
		response.subscribe(ts);
		// these only test client side since this is fireAndForgetOrMetadataPush
		ts.await();
		ts.assertNoError();
		ts.assertComplete();
		// this waits for server-side
		fireAndForgetOrMetadataPush.await();
		assertEquals("log", lastFireAndForget.get());
	}

	@Test(timeout=2000)
	@Theory
	public void testFireAndForgetServerSideErrorNotFound(int setupFlag) throws InterruptedException {
		startSockets(setupFlag);
		// perform request/response

		Publisher<Void> response = socketClient.fireAndForget(TestUtil.utf8EncodedPayload("unknown", null));
		TestSubscriber<Void> ts = TestSubscriber.create();
		response.subscribe(ts);
		// these only test client side since this is fireAndForgetOrMetadataPush
		ts.await();
		ts.assertNoError();// client-side won't see an error
		ts.assertComplete();
		// this waits for server-side
		fireAndForgetOrMetadataPush.await();
		assertEquals("notFound", lastFireAndForget.get());
	}

	@Test(timeout=2000)
	@Theory
	public void testFireAndForgetServerSideErrorHandlerBlowup(int setupFlag) throws InterruptedException {
		startSockets(setupFlag);
		// perform request/response

		Publisher<Void> response = socketClient.fireAndForget(TestUtil.utf8EncodedPayload("blowup", null));
		TestSubscriber<Void> ts = TestSubscriber.create();
		response.subscribe(ts);
		// these only test client side since this is fireAndForgetOrMetadataPush
		ts.await();
		ts.assertNoError();// client-side won't see an error
		ts.assertComplete();
		// this waits for server-side
		fireAndForgetOrMetadataPush.await();
		assertEquals("blowup", lastFireAndForget.get());
		lastServerErrorCountDown.await();
		assertEquals("forced blowup to simulate handler error", lastServerError.get().getCause().getMessage());
	}

	@Test(timeout=2000)
	@Theory
	public void testRequestChannelEcho(int setupFlag) throws InterruptedException {
		startSockets(setupFlag);

		Publisher<Payload> inputs = Flux.just(
				TestUtil.utf8EncodedPayload("1", "echo"),
				TestUtil.utf8EncodedPayload("2", "echo")
		);
		Publisher<Payload> outputs = socketClient.requestChannel(inputs);
		TestSubscriber<Payload> ts = TestSubscriber.create();
		outputs.subscribe(ts);
		ts.await();
		ts.assertNoError();
		ts.assertValueCount(2);
		ts.assertValuesWith(
			value -> assertEquals("1_echo", byteToString(value.getData())),
			value -> assertEquals("2_echo", byteToString(value.getData()))
		);
	}

	@Test(timeout=2000)
	@Theory
	public void testRequestChannelNotFound(int setupFlag) throws InterruptedException {
		startSockets(setupFlag);

		Publisher<Payload> requestStream = Flux.just(TestUtil.utf8EncodedPayload(null, "someChannel"));
		Publisher<Payload> response = socketClient.requestChannel(requestStream);
		TestSubscriber<Payload> ts = TestSubscriber.create();
		response.subscribe(ts);
		ts.await();
		ts.assertTerminated();
		ts.assertNotComplete();
		ts.assertNoValues();
		ts.assertErrorMessage("Not Found");
	}

	@Test(timeout=2000)
	@Theory
	public void testMetadataPushSuccess(int setupFlag) throws InterruptedException {
		startSockets(setupFlag);

		// perform request/response

		Publisher<Void> response = socketClient.metadataPush(TestUtil.utf8EncodedPayload(null, "log"));
		TestSubscriber<Void> ts = TestSubscriber.create();
		response.subscribe(ts);
		ts.await();
		ts.assertNoError();
		ts.assertComplete();
		// this waits for server-side
		fireAndForgetOrMetadataPush.await();
		assertEquals("log", lastMetadataPush.get());
	}

	@Test(timeout=2000)
	@Theory
	public void testMetadataPushServerSideErrorNotFound(int setupFlag) throws InterruptedException {
		startSockets(setupFlag);
		// perform request/response

		Publisher<Void> response = socketClient.metadataPush(TestUtil.utf8EncodedPayload(null, "unknown"));
		TestSubscriber<Void> ts = TestSubscriber.create();
		response.subscribe(ts);
		ts.await();
		ts.assertNoError();// client-side won't see an error
		ts.assertComplete();
		// this waits for server-side
		fireAndForgetOrMetadataPush.await();
		assertEquals("notFound", lastMetadataPush.get());
	}

	@Test(timeout=2000)
	@Theory
	public void testMetadataPushServerSideErrorHandlerBlowup(int setupFlag) throws InterruptedException {
		startSockets(setupFlag);
		// perform request/response

		Publisher<Void> response = socketClient.metadataPush(TestUtil.utf8EncodedPayload(null, "blowup"));
		TestSubscriber<Void> ts = TestSubscriber.create();
		response.subscribe(ts);
		ts.await();
		ts.assertNoError();// client-side won't see an error
		ts.assertComplete();
		// this waits for server-side
		fireAndForgetOrMetadataPush.await();
		assertEquals("blowup", lastMetadataPush.get());
		lastServerErrorCountDown.await();
		assertEquals("forced blowup to simulate handler error", lastServerError.get().getCause().getMessage());
	}
	
	@Test(timeout=2000)
	@Theory
	public void testServerRequestResponse(int setupFlag) throws InterruptedException {
		startSockets(setupFlag, new RequestHandler.Builder().withRequestResponse(payload ->
			Flux.just(utf8EncodedPayload("hello world from client", null))
		).build());

		CountDownLatch latch = new CountDownLatch(1);
		socketServer.onRequestReady(err -> {
			latch.countDown();
		});
		latch.await();
		
		Publisher<Payload> response = socketServer.requestResponse(TestUtil.utf8EncodedPayload("hello", null));
		TestSubscriber<Payload> ts = TestSubscriber.create();
		response.subscribe(ts);
		ts.await();
		ts.assertNoError();
		ts.assertValuesWith(value ->
			assertEquals(TestUtil.utf8EncodedPayload("hello world from client", null), value)
		);
	}
	
	
}
