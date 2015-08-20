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

import static rx.Observable.*;
import static io.reactivesocket.TestUtil.*;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.reactivestreams.Publisher;

import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;

import static rx.RxReactiveStreams.toObservable;
import static rx.RxReactiveStreams.toPublisher;

import java.util.concurrent.TimeUnit;

public class ReactiveSocketTest {

	private static TestConnection serverConnection;
	private static TestConnection clientConnection;
	private static ReactiveSocket socketServer;
	private static ReactiveSocket socketClient;

	@BeforeClass
	public static void setup() {
		serverConnection = new TestConnection();
		clientConnection = new TestConnection();
		clientConnection.connectToServerConnection(serverConnection);

		socketServer = ReactiveSocket.createResponderAndRequestor(new RequestHandler() {

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
			public Publisher<Payload> handleRequestSubscription(Payload payload) {
				return toPublisher(error(new RuntimeException("Not Found")));
			}

			@Override
			public Publisher<Void> handleFireAndForget(Payload payload) {
				return toPublisher(error(new RuntimeException("Not Found")));
			}

		});

		socketClient = ReactiveSocket.createRequestor();

		// start both the server and client and monitor for errors
		toObservable(socketServer.connect(serverConnection)).subscribe();
		toObservable(socketClient.connect(clientConnection)).subscribe();
	}

	@Test
	public void testRequestResponse() {
		// perform request/response
		Publisher<Payload> response = socketClient.requestResponse(TestUtil.utf8EncodedPayload("hello", null));
		TestSubscriber<Payload> ts = TestSubscriber.create();
		toObservable(response).subscribe(ts);
		ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
		ts.assertNoErrors();
		ts.assertValue(TestUtil.utf8EncodedPayload("hello world", null));
	}

	@Test
	public void testRequestStream() {
		// perform request/stream
		Publisher<Payload> response = socketClient.requestStream(TestUtil.utf8EncodedPayload("hello", null));
		TestSubscriber<Payload> ts = TestSubscriber.create();
		toObservable(response).subscribe(ts);
		ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
		ts.assertNoErrors();
		assertEquals(100, ts.getOnNextEvents().size());
		assertEquals("hello world 99", byteToString(ts.getOnNextEvents().get(99).getData()));
	}
}
