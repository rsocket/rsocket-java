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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.reactivestreams.Publisher;

import io.reactivex.subscribers.TestSubscriber;

public class TestFragmentationRequests {
	static String LARGE_STRING = "";
	
	static {
		for(int i=0; i < 1000; i++) {
			LARGE_STRING += "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";
		}
	}
	
	@Test(timeout=10000)
	public void testRequestResponseFragmentationResponse() throws InterruptedException {
		AtomicInteger framesWritten = new AtomicInteger();
		serverConnection.write.add(frame -> framesWritten.incrementAndGet());
		
		AtomicInteger framesReceived = new AtomicInteger();
		clientConnection.toInput.add(frame -> framesReceived.incrementAndGet());
		
		TestSubscriber<Payload> ts = new TestSubscriber<>();
		socketClient.requestResponse(utf8EncodedPayload("", null))
			.subscribe(ts);
		ts.await();
		
		Thread.sleep(500);
		if(framesWritten.get() <= 1) {
			fail("Expected multiple fragments");
		}
		
		ts.assertValueCount(1);
		assertEquals(LARGE_STRING, TestUtil.byteToString(ts.values().get(0).getData()));
	}
	
	@Test(timeout=10000)
	public void testRequestStreamFragmentationResponse() throws InterruptedException {
		AtomicInteger framesWritten = new AtomicInteger();
		serverConnection.write.add(frame -> framesWritten.incrementAndGet());
		
		AtomicInteger framesReceived = new AtomicInteger();
		clientConnection.toInput.add(frame -> framesReceived.incrementAndGet());
		
		TestSubscriber<Payload> ts = new TestSubscriber<>();
		socketClient.requestStream(utf8EncodedPayload("", null))
			.subscribe(ts);
		ts.await();
		
		Thread.sleep(500);
		if(framesWritten.get() <= 2) {
			fail("Expected more than 2 Frames");
		}
		
		ts.assertValueCount(2);
		assertEquals(LARGE_STRING, TestUtil.byteToString(ts.values().get(0).getData()));
		assertEquals(LARGE_STRING, TestUtil.byteToString(ts.values().get(1).getData()));
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
	public static void setup() throws InterruptedException {
		serverConnection = new TestConnection();
		clientConnection = new TestConnection();
		clientConnection.connectToServerConnection(serverConnection, true);
		

		socketServer = ReactiveSocket.fromServerConnection(serverConnection, setup -> new RequestHandler() {

			@Override
			public Publisher<Payload> handleRequestStream(Payload payload) {
				return just(utf8EncodedPayload(LARGE_STRING, null), utf8EncodedPayload(LARGE_STRING, null));
			}

			@Override
			public Publisher<Payload> handleSubscription(Payload payload) {
				return error(new RuntimeException("Not Found"));
			}

			@Override
			public Publisher<Payload> handleChannel(Payload initialPayload, Publisher<Payload> payloads) {
				return error(new RuntimeException("Not Found"));
			}

			@Override
			public Publisher<Void> handleFireAndForget(Payload payload) {
				return error(new RuntimeException("Not Found"));
			}

			@Override
			public Publisher<Payload> handleRequestResponse(Payload payload) {
				return just(utf8EncodedPayload(LARGE_STRING, null));
			}

			@Override
			public Publisher<Void> handleMetadataPush(Payload payload)
			{
				return error(new RuntimeException("Not Found"));
			}
		}, LeaseGovernor.UNLIMITED_LEASE_GOVERNOR, Throwable::printStackTrace);

		socketClient = ReactiveSocket.fromClientConnection(clientConnection, ConnectionSetupPayload.create("UTF-8", "UTF-8", NO_FLAGS), Throwable::printStackTrace);

        socketServer.startAndWait();
        socketClient.startAndWait();
	}

	@AfterClass
	public static void shutdown() {
		socketServer.shutdown();
		socketClient.shutdown();
	}
}
