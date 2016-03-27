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

import static io.reactivesocket.ConnectionSetupPayload.*;
import static io.reactivesocket.TestUtil.*;
import static io.reactivex.Observable.*;
import static org.junit.Assert.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.reactivestreams.Publisher;

import io.reactivesocket.lease.FairLeaseGovernor;
import io.reactivex.disposables.Disposable;
import io.reactivex.observables.ConnectableObservable;
import io.reactivex.subscribers.TestSubscriber;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

@RunWith(Theories.class)
public class ReactiveSocketTest {

	private TestConnection clientConnection;
	private ReactiveSocket socketServer;
	private ReactiveSocket socketClient;
	private AtomicBoolean helloSubscriptionRunning = new AtomicBoolean(false);
	private AtomicReference<String> lastFireAndForget = new AtomicReference<String>();
	private AtomicReference<String> lastMetadataPush = new AtomicReference<String>();
	private AtomicReference<Throwable> lastServerError = new AtomicReference<Throwable>();
	private CountDownLatch lastServerErrorCountDown;
	private CountDownLatch fireAndForgetOrMetadataPush;

	public static @DataPoints int[] setupFlags = {NO_FLAGS, HONOR_LEASE};

	@Before
	public void setup() {
		TestConnection serverConnection = new TestConnection();
		clientConnection = new TestConnection();
		clientConnection.connectToServerConnection(serverConnection);
		fireAndForgetOrMetadataPush = new CountDownLatch(1);
		lastServerErrorCountDown = new CountDownLatch(1);

		socketServer = ReactiveSocketImpl.fromServerConnection(serverConnection, setup -> new RequestHandler() {

			@Override
			public Publisher<Payload> handleRequestResponse(Payload payload) {
				String request = byteToString(payload.getData());
				System.out.println("********************************************************************************************** requestResponse: " + request);
				if ("hello".equals(request)) {
					System.out.println("********************************************************************************************** respond hello");
					return just(utf8EncodedPayload("hello world", null));
				} else {
					return error(new RuntimeException("Not Found"));
				}
			}

			@Override
			public Publisher<Payload> handleRequestStream(Payload payload) {
				String request = byteToString(payload.getData());
				if ("hello".equals(request)) {
					return range(0, 100).map(i -> "hello world " + i).map(n -> utf8EncodedPayload(n, null));
				} else {
					return error(new RuntimeException("Not Found"));
				}
			}

			@Override
			public Publisher<Payload> handleSubscription(Payload payload) {
				String request = byteToString(payload.getData());
				if ("hello".equals(request)) {
					return interval(1, TimeUnit.MICROSECONDS)
						.onBackpressureDrop()
						.doOnSubscribe(s -> helloSubscriptionRunning.set(true))
						.doOnCancel(() -> helloSubscriptionRunning.set(false))
						.map(i -> "subscription " + i)
						.map(n -> utf8EncodedPayload(n, null));
				} else {
					return error(new RuntimeException("Not Found"));
				}
			}

			@Override
			public Publisher<Void> handleFireAndForget(Payload payload) {
				try {
					String request = byteToString(payload.getData());
					lastFireAndForget.set(request);
					if ("log".equals(request)) {
						return empty(); // success
					} else if ("blowup".equals(request)) {
						throw new RuntimeException("forced blowup to simulate handler error");
					} else {
						lastFireAndForget.set("notFound");
						return error(new RuntimeException("Not Found"));
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
						return empty(); // success
					} else if ("blowup".equals(request)) {
						throw new RuntimeException("forced blowup to simulate handler error");
					} else {
						lastMetadataPush.set("notFound");
						return error(new RuntimeException("Not Found"));
					}
				} finally {
					fireAndForgetOrMetadataPush.countDown();
				}
			}

			private Publisher<Payload> echoChannel(Publisher<Payload> echo) {
				return fromPublisher(echo).map(p -> {
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
		socketClient = ReactiveSocketImpl.fromClientConnection(
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

	@Test(timeout=2000)
	@Theory
	public void testRequestResponse(int setupFlag) throws InterruptedException {
		startSockets(setupFlag);
		// perform request/response

		Publisher<Payload> response = socketClient.requestResponse(TestUtil.utf8EncodedPayload("hello", null));
		TestSubscriber<Payload> ts = new TestSubscriber<>();
		response.subscribe(ts);
		ts.awaitTerminalEvent();
		ts.assertNoErrors();
		ts.assertValue(TestUtil.utf8EncodedPayload("hello world", null));
	}
	
	@Test(timeout=2000, expected=IllegalStateException.class)
	public void testRequestResponsePremature() throws InterruptedException {
		socketClient = ReactiveSocketImpl.fromClientConnection(
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
		TestSubscriber<Payload> ts = new TestSubscriber<>();
		response.subscribe(ts);
		ts.awaitTerminalEvent();
		ts.assertNoErrors();
		assertEquals(100, ts.values().size());
		assertEquals("hello world 99", byteToString(ts.values().get(99).getData()));
	}

	@Test(timeout=4000)
	@Theory
	public void testRequestSubscription(int setupFlag) throws InterruptedException {
		startSockets(setupFlag);
		// perform request/subscription

		Publisher<Payload> response = socketClient.requestSubscription(TestUtil.utf8EncodedPayload("hello", null));
		TestSubscriber<Payload> ts = new TestSubscriber<>();
		TestSubscriber<Payload> ts2 = new TestSubscriber<>();
		ConnectableObservable<Payload> published = fromPublisher(response).publish();
		published.take(10).subscribe(ts);
		published.subscribe(ts2);
		Disposable subscription = published.connect();

		// ts completed due to take
		ts.awaitTerminalEvent();
		ts.assertNoErrors();
		ts.assertComplete();

		// ts2 should never complete
		ts2.assertNoErrors();
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

		assertEquals(10, ts.values().size());
		assertEquals("subscription 9", byteToString(ts.values().get(9).getData()));
	}

	@Test(timeout=2000)
	@Theory
	public void testFireAndForgetSuccess(int setupFlag) throws InterruptedException {
		startSockets(setupFlag);

		// perform request/response

		Publisher<Void> response = socketClient.fireAndForget(TestUtil.utf8EncodedPayload("log", null));
		TestSubscriber<Void> ts = new TestSubscriber<>();
		response.subscribe(ts);
		// these only test client side since this is fireAndForgetOrMetadataPush
		ts.awaitTerminalEvent();
		ts.assertNoErrors();
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
		TestSubscriber<Void> ts = new TestSubscriber<>();
		response.subscribe(ts);
		// these only test client side since this is fireAndForgetOrMetadataPush
		ts.awaitTerminalEvent();
		ts.assertNoErrors();// client-side won't see an error
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
		TestSubscriber<Void> ts = new TestSubscriber<>();
		response.subscribe(ts);
		// these only test client side since this is fireAndForgetOrMetadataPush
		ts.awaitTerminalEvent();
		ts.assertNoErrors();// client-side won't see an error
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

		Publisher<Payload> inputs = just(
				TestUtil.utf8EncodedPayload("1", "echo"),
				TestUtil.utf8EncodedPayload("2", "echo")
		);
		Publisher<Payload> outputs = socketClient.requestChannel(inputs);
		TestSubscriber<Payload> ts = new TestSubscriber<>();
		outputs.subscribe(ts);
		ts.awaitTerminalEvent();
		ts.assertNoErrors();
		assertEquals(2, ts.values().size());
		assertEquals("1_echo", byteToString(ts.values().get(0).getData()));
		assertEquals("2_echo", byteToString(ts.values().get(1).getData()));
	}

	@Test(timeout=2000)
	@Theory
	public void testRequestChannelNotFound(int setupFlag) throws InterruptedException {
		startSockets(setupFlag);

		Publisher<Payload> requestStream = just(TestUtil.utf8EncodedPayload(null, "someChannel"));
		Publisher<Payload> response = socketClient.requestChannel(requestStream);
		TestSubscriber<Payload> ts = new TestSubscriber<>();
		response.subscribe(ts);
		ts.awaitTerminalEvent();
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
		TestSubscriber<Void> ts = new TestSubscriber<>();
		response.subscribe(ts);
		ts.awaitTerminalEvent();
		ts.assertNoErrors();
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
		TestSubscriber<Void> ts = new TestSubscriber<>();
		response.subscribe(ts);
		ts.awaitTerminalEvent();
		ts.assertNoErrors();// client-side won't see an error
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
		TestSubscriber<Void> ts = new TestSubscriber<>();
		response.subscribe(ts);
		ts.awaitTerminalEvent();
		ts.assertNoErrors();// client-side won't see an error
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
		startSockets(setupFlag, new RequestHandler.Builder()
				.withRequestResponse(payload -> {
					return just(utf8EncodedPayload("hello world from client", null));
				}).build());

		CountDownLatch latch = new CountDownLatch(1);
		socketServer.onRequestReady(err -> {
			latch.countDown();
		});
		latch.await();
		
		Publisher<Payload> response = socketServer.requestResponse(TestUtil.utf8EncodedPayload("hello", null));
		TestSubscriber<Payload> ts = new TestSubscriber<>();
		response.subscribe(ts);
		ts.awaitTerminalEvent();
		ts.assertNoErrors();
		ts.assertValue(TestUtil.utf8EncodedPayload("hello world from client", null));
	}
	
	
}
