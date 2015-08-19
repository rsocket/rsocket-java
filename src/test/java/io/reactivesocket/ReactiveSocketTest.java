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

	@Before
	public void setup() {
		serverConnection = new TestConnection();
		serverConnection.writes.forEach(n -> System.out.println("SERVER ==> Writes from server->client: " + n));
		serverConnection.toInput.forEach(n -> System.out.println("SERVER <== Input from client->server: " + n));
		clientConnection = new TestConnection();
		clientConnection.writes.forEach(n -> System.out.println("CLIENT ==> Writes from client->server: " + n));
		clientConnection.toInput.forEach(n -> System.out.println("CLIENT <== Input from server->client: " + n));

		// connect the connections (with a Scheduler to simulate async IO)
		clientConnection.writes
//				.subscribeOn(Schedulers.computation())
//				.observeOn(Schedulers.computation())
				.subscribe(serverConnection.toInput);
		serverConnection.writes
//				.subscribeOn(Schedulers.computation())
//				.observeOn(Schedulers.computation())
				.subscribe(clientConnection.toInput);

	}

	@Test
	public void testRequestResponse() {
		// server
		ReactiveSocket.createResponderAndRequestor(serverConnection, new RequestHandler() {

			@Override
			public Publisher<Payload> handleRequestResponse(Payload payload) {
				System.out.println("... handling request: " + payload);
				String request = byteToString(payload.getData());
				if ("hello".equals(request)) {
					return toPublisher(just(utf8EncodedPayload("hello world", null)));
				} else {
					return toPublisher(error(new RuntimeException("Not Found")));
				}
			}

			@Override
			public Publisher<Payload> handleRequestStream(Payload payload) {
				return toPublisher(error(new RuntimeException("Not Found")));
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

		// client
		ReactiveSocket client = ReactiveSocket.createRequestor(clientConnection);
		Publisher<Payload> response = client.requestResponse(TestUtil.utf8EncodedPayload("hello", null));
		TestSubscriber<Payload> ts = TestSubscriber.create();
		toObservable(response).subscribe(ts);
		ts.awaitTerminalEvent(500, TimeUnit.MILLISECONDS);
		ts.assertNoErrors();
		ts.assertValue(TestUtil.utf8EncodedPayload("hello world", null));
	}
}
