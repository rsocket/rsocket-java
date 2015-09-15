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

import static io.reactivex.Observable.*;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import org.reactivestreams.Publisher;

import io.reactivesocket.observable.Observer;
import io.reactivex.Observable;
import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;

public class TestConnection implements DuplexConnection {

	public final Channel toInput = new Channel();
	public final Channel write = new Channel();

	@Override
	public void addOutput(Publisher<Frame> o, Completable callback) {
		fromPublisher(o).flatMap(m -> {
			// no backpressure on a Subject so just firehosing for this test
			write.send(m);
			return Observable.<Void> empty();
		}).subscribe(v -> {}, callback::error, callback::success);
	}

	@Override
	public io.reactivesocket.observable.Observable<Frame> getInput() {
		return new io.reactivesocket.observable.Observable<Frame>() {

			@Override
			public void subscribe(Observer<Frame> o) {
				toInput.add(o);
				// we are okay with the race of sending data and cancelling ... since this is "hot" by definition and unsubscribing is a race.
				o.onSubscribe(new io.reactivesocket.observable.Disposable() {

					@Override
					public void dispose() {
						toInput.remove(o);
					}

				});
			}

		};
	}

	public void connectToServerConnection(TestConnection serverConnection) {
		connectToServerConnection(serverConnection, true);
	}

	public void connectToServerConnection(TestConnection serverConnection, boolean log) {
		if (log) {
			serverConnection.write.add(n -> System.out.println("SERVER ==> Writes from server->client: " + n + "   Written from " + Thread.currentThread()));
			serverConnection.toInput.add(n -> System.out.println("SERVER <== Input from client->server: " + n + "   Read on " + Thread.currentThread()));
			write.add(n -> System.out.println("CLIENT ==> Writes from client->server: " + n + "   Written from " + Thread.currentThread()));
			toInput.add(n -> System.out.println("CLIENT <== Input from server->client: " + n + "   Read on " + Thread.currentThread()));
		}
		
		// client to server
		write.add(f -> serverConnection.toInput.send(f));	
		// server to client
		serverConnection.write.add(f -> toInput.send(f));	
	}

	@Override
	public void close() throws IOException {

	}

	
	public static class Channel {

		private final CopyOnWriteArrayList<Observer<Frame>> os = new CopyOnWriteArrayList<>();

		public void send(Frame f) {
			for (Observer<Frame> o : os) {
				o.onNext(f);
			}
		}

		public void add(Observer<Frame> o) {
			os.add(o);
		}

		public void add(Consumer<Frame> f) {
			add(new Observer<Frame>() {

				@Override
				public void onNext(Frame t) {
					f.accept(t);
				}

				@Override
				public void onError(Throwable e) {

				}

				@Override
				public void onComplete() {

				}

				@Override
				public void onSubscribe(io.reactivesocket.observable.Disposable d) {
					// TODO Auto-generated method stub

				}

			});
		}

		public void remove(Observer<Frame> o) {
			os.remove(o);
		}
	}

	
}