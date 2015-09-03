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

import static rx.RxReactiveStreams.*;

import java.io.IOException;

import org.reactivestreams.Publisher;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

public class TestConnection implements DuplexConnection {

	public final PublishSubject<Frame> toInput = PublishSubject.create();
	private PublishSubject<Frame> writeSubject = PublishSubject.create();
	public final Observable<Frame> writes = writeSubject;

	@Override
	public void addOutput(Publisher<Frame> o, Completable callback) {
		toObservable(o).flatMap(m -> {
			// no backpressure on a Subject so just firehosing for this test
			writeSubject.onNext(m);
			return Observable.<Void> empty();
		}).subscribe(v -> {}, callback::error, callback::success);
	}

	@Override
	public Publisher<Frame> getInput() {
		return toPublisher(toInput);
	}

	public void connectToServerConnection(TestConnection serverConnection) {
		connectToServerConnection(serverConnection, true);
	}
	
	public void connectToServerConnection(TestConnection serverConnection, boolean log) {
		if (log) {
			serverConnection.writes.forEach(n -> System.out.println("SERVER ==> Writes from server->client: " + n + " " + Thread.currentThread()));
			serverConnection.toInput.forEach(n -> System.out.println("SERVER <== Input from client->server: " + n + " " + Thread.currentThread()));
			writes.forEach(n -> System.out.println("CLIENT ==> Writes from client->server: " + n + " " + Thread.currentThread()));
			toInput.forEach(n -> System.out.println("CLIENT <== Input from server->client: " + n + " " + Thread.currentThread()));
		}

		Scheduler clientThread = Schedulers.newThread();
		Scheduler serverThread = Schedulers.newThread();
		
		// TODO commented out because we have concurrency issues ... need to restore the scheduling and get everything working
		
		// connect the connections (with a Scheduler to simulate async IO)
		writes
//			.subscribeOn(clientThread)
//			.onBackpressureBuffer() // simulate unbounded network buffer
//			.observeOn(serverThread)
			.subscribe(serverConnection.toInput);
		serverConnection.writes
//			.subscribeOn(serverThread)
//			.onBackpressureBuffer() // simulate unbounded network buffer
//			.observeOn(clientThread)
			.subscribe(toInput);

	}

	@Override
	public void close() throws IOException {
		
	}
}