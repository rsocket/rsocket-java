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
import static org.junit.Assert.*;
import static rx.Observable.*;
import static rx.RxReactiveStreams.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class TestFlowControlRequestN {

	@Test(timeout=2000)
	public void testRequestStream_batches() throws InterruptedException {
		ControlledSubscriber s = new ControlledSubscriber();
		socketClient.requestStream(utf8EncodedPayload("100", null)).subscribe(s);
		assertEquals(0, s.received.get());
		assertEquals(0, emitted.get());
		s.subscription.request(10);
		waitForAsyncValue(s.received, 10);
		assertEquals(10, s.received.get());
		assertEquals(10, emitted.get());
		s.subscription.request(50);
		waitForAsyncValue(s.received, 60);
		assertEquals(60, s.received.get());
		assertEquals(60, emitted.get());
		s.subscription.request(100);
		waitForAsyncValue(s.received, 100);
		assertEquals(100, s.received.get());
		s.terminated.await();
		assertEquals(100, emitted.get());
		
		assertTrue(s.completed.get());
	}
	
	@Test(timeout=3000)
	public void testRequestStream_fastProducer_slowConsumer_maxValueRequest() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);
		CountDownLatch cancelled = new CountDownLatch(1);
		AtomicInteger received = new AtomicInteger();
		socketClient.requestStream(utf8EncodedPayload("10000", null)).subscribe(new Subscriber<Payload>() {

			Subscription subscription;
			@Override
			public void onSubscribe(Subscription s) {
				subscription = s;
				s.request(Long.MAX_VALUE); // act like a synchronous consumer that doesn't need backpressure
			}

			@Override
			public void onNext(Payload t) {
				int r = received.incrementAndGet();
				System.out.println("onNext " + r);
				if(r == 10) {
					// be a "slow" consumer
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
					}
					System.out.println("Emitted on server: " + emitted.get() + " Received on client: " + received);
				} else if(r == 200) {
					System.out.println("Cancel");
					// cancel
					subscription.cancel();
					cancelled.countDown();
					onComplete();
				}
			}

			@Override
			public void onError(Throwable t) {
				t.printStackTrace();
				latch.countDown();				
			}

			@Override
			public void onComplete() {
				System.out.println("complete");
				latch.countDown();				
			}
			
		});
		
		System.out.println("waiting");
		latch.await(3000, TimeUnit.MILLISECONDS);
		cancelled.await(3000, TimeUnit.MILLISECONDS);
		assertEquals(200, received.get());
		if(emitted.get() > 1024) {
			fail("Emitted more than expected");
		}
	}
	
	@Test(timeout=2000)
	public void testRequestSubscription_batches() throws InterruptedException {
		ControlledSubscriber s = new ControlledSubscriber();
		socketClient.requestSubscription(utf8EncodedPayload("", null)).subscribe(s);
		assertEquals(0, s.received.get());
		assertEquals(0, emitted.get());
		s.subscription.request(10);
		waitForAsyncValue(s.received, 10);
		assertEquals(10, s.received.get());
		assertEquals(10, emitted.get());
		s.subscription.request(50);
		waitForAsyncValue(s.received, 60);
		assertEquals(60, s.received.get());
		assertEquals(60, emitted.get());
		s.subscription.request(100);
		waitForAsyncValue(s.received, 160);
		assertEquals(160, s.received.get());
		s.subscription.cancel();
		Thread.sleep(100);
		assertEquals(160, emitted.get());
	}
	
	/**
	 * Test that downstream is governed by request(n)
	 * @throws InterruptedException 
	 */
	@Test(timeout=2000)
	public void testRequestChannel_batches_downstream() throws InterruptedException {
		ControlledSubscriber s = new ControlledSubscriber();
		socketClient.requestChannel(toPublisher(
				range(1, 10)
				.map(i -> {
					return utf8EncodedPayload(String.valueOf(i), "1000"); 
				}))).subscribe(s);
		
		// if flatMap is being used, then each of the 10 streams will emit at least 128 (default)
		
		assertEquals(0, s.received.get());
		assertEquals(0, emitted.get());
		s.subscription.request(10);
		waitForAsyncValue(s.received, 10);
		assertEquals(10, s.received.get());
		s.subscription.request(300);
		waitForAsyncValue(s.received, 310);
		assertEquals(310, s.received.get());
		s.subscription.request(2000);
		waitForAsyncValue(s.received, 2310);
		assertEquals(2310, s.received.get());
		s.subscription.cancel();
		Thread.sleep(100);
		assertEquals(2310, s.received.get());
		// emitted with `flatMap` does internal buffering, so it won't be exactly 2310, but it should be far less than the potential 10,000
		if(emitted.get() > 4096) {
			fail("Emitted " + emitted.get());
		}
	}
	
	/**
	 * Test that the upstream is governed by request(n)
	 */
	@Ignore // TODO
	@Test
	public void testRequestChannel_batches_upstream() {
		ControlledSubscriber s = new ControlledSubscriber();
		socketClient.requestChannel(toPublisher(
				range(1, 10000)
				.map(i -> {
					return utf8EncodedPayload(String.valueOf(i), "echo"); // metadata to route us to the echo behavior (only actually need this in the first payload) 
				}))).subscribe(s);
		
		assertEquals(0, s.received);
		assertEquals(0, emitted.get());
		s.subscription.request(10);
		assertFalse(s.error.get());
	}

	private void waitForAsyncValue(AtomicInteger value, int n) throws InterruptedException {
		while (value.get() != n && !Thread.interrupted()) {
			Thread.sleep(1);
		}
	}
	
	private static class ControlledSubscriber implements Subscriber<Payload> {

		AtomicInteger received = new AtomicInteger();
		Subscription subscription;
		CountDownLatch terminated = new CountDownLatch(1);
		AtomicBoolean completed = new AtomicBoolean(false);
		AtomicBoolean error = new AtomicBoolean(false);
		
		@Override
		public void onSubscribe(Subscription s) {
			this.subscription = s;
		}

		@Override
		public void onNext(Payload t) {
			received.incrementAndGet();
		}

		@Override
		public void onError(Throwable t) {
			t.printStackTrace();
			error.set(true);
			terminated.countDown();
		}

		@Override
		public void onComplete() {
			completed.set(true);
			terminated.countDown();
		}
		
	}
	
	private static TestConnection serverConnection;
	private static TestConnection clientConnection;
	private static ReactiveSocket socketServer;
	private static ReactiveSocket socketClient;
	private static AtomicInteger emitted = new AtomicInteger();
	private static AtomicInteger numRequests = new AtomicInteger();
	private static AtomicLong requested = new AtomicLong();
	
	@Before
	public void init() {
		emitted.set(0);
		requested.set(0);
		numRequests.set(0);
	}

	@BeforeClass
	public static void setup() {
		serverConnection = new TestConnection();
		clientConnection = new TestConnection();
		clientConnection.connectToServerConnection(serverConnection, false);
		

		socketServer = ReactiveSocket.fromServerConnection(serverConnection, setup -> new RequestHandler() {

			@Override
			public Publisher<Payload> handleRequestStream(Payload payload) {
				String request = byteToString(payload.getData());
				System.out.println("responder received requestStream: " + request);
				return toPublisher(range(0, Integer.parseInt(request))
						.doOnRequest(n -> System.out.println("requested in responder: " + n))
						.doOnRequest(r -> requested.addAndGet(r))
						.doOnRequest(r -> numRequests.incrementAndGet())
						.doOnNext(i -> emitted.incrementAndGet())
						.map(i -> utf8EncodedPayload(String.valueOf(i), null)));
			}

			@Override
			public Publisher<Payload> handleSubscription(Payload payload) {
				return toPublisher(range(0, Integer.MAX_VALUE)
						.doOnRequest(n -> System.out.println("requested in responder: " + n))
						.doOnRequest(r -> requested.addAndGet(r))
						.doOnRequest(r -> numRequests.incrementAndGet())
						.doOnNext(i -> emitted.incrementAndGet())
						.map(i -> utf8EncodedPayload(String.valueOf(i), null)));
			}

			/**
			 * Use Payload.metadata for routing
			 */
			@Override
			public Publisher<Payload> handleChannel(Payload initialPayload, Publisher<Payload> payloads) {
				String requestMetadata = byteToString(initialPayload.getMetadata());
				System.out.println("responder received requestChannel: " + requestMetadata);
				
				if(requestMetadata.equals("echo")) {
					return toPublisher(toObservable(payloads).map(payload -> { // TODO I want this to be concatMap instead of flatMap but apparently concatMap has a bug
						String payloadData = byteToString(payload.getData());
						return utf8EncodedPayload(String.valueOf(payloadData) + "_echo", null);	
					}).doOnRequest(n -> System.out.println("requested in echo responder: " + n))
					  .doOnRequest(r -> requested.addAndGet(r))
					  .doOnRequest(r -> numRequests.incrementAndGet())
					  .doOnError(t -> System.out.println("Error in 'echo' handler: " + t.getMessage()))
					  .doOnNext(i -> emitted.incrementAndGet()));
				} else {
					return toPublisher(toObservable(payloads).flatMap(payload -> { // TODO I want this to be concatMap instead of flatMap but apparently concatMap has a bug
						String payloadData = byteToString(payload.getData());
						System.out.println("responder handleChannel received payload: " + payloadData);
						return range(0, Integer.parseInt(requestMetadata))
								.doOnRequest(n -> System.out.println("requested in responder [" + payloadData + "]: " + n))
								.doOnRequest(r -> requested.addAndGet(r))
								.doOnRequest(r -> numRequests.incrementAndGet())
								.doOnNext(i -> emitted.incrementAndGet())
								.map(i -> utf8EncodedPayload(String.valueOf(i), null));	
					}).doOnRequest(n -> System.out.println(">>> response stream request(n) in responder: " + n)));
				}
			}

			@Override
			public Publisher<Void> handleFireAndForget(Payload payload) {
				return toPublisher(error(new RuntimeException("Not Found")));
			}

			@Override
			public Publisher<Payload> handleRequestResponse(Payload payload) {
				return toPublisher(error(new RuntimeException("Not Found")));
			}

		}, LeaseGovernor.UNLIMITED_LEASE_GOVERNOR, t -> t.printStackTrace());

		socketClient = ReactiveSocket.fromClientConnection(clientConnection, ConnectionSetupPayload.create("UTF-8", "UTF-8", NO_FLAGS), t -> t.printStackTrace());

		// start both the server and client and monitor for errors
		socketServer.start();
		socketClient.start();
	}

	@AfterClass
	public static void shutdown() {
		socketServer.shutdown();
		socketClient.shutdown();
	}

}
