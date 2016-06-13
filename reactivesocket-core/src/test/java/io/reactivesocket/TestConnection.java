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

import java.io.IOException;

import org.reactivestreams.Publisher;

import io.reactivesocket.rx.Completable;
import io.reactivesocket.rx.Observer;
import reactor.core.flow.Cancellation;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Scheduler.Worker;
import reactor.core.scheduler.Schedulers;

public class TestConnection implements DuplexConnection {

	public final SerializedEventBus toInput = new SerializedEventBus();
	public final SerializedEventBus write = new SerializedEventBus();

	@Override
	public void addOutput(Publisher<Frame> o, Completable callback) {
		Flux.from(o).flatMap(m -> {
			// no backpressure on a Subject so just firehosing for this test
			write.send(m);
			return Flux.empty();
		}).subscribe(v -> {
		} , callback::error, callback::success);
	}

	@Override
	public void addOutput(Frame f, Completable callback) {
        write.send(f);
        callback.success();
	}

	@Override
	public double availability() {
		return 1.0;
	}

	@Override
	public io.reactivesocket.rx.Observable<Frame> getInput() {
		return new io.reactivesocket.rx.Observable<Frame>() {

			@Override
			public void subscribe(Observer<Frame> o) {
				toInput.add(o);
				// we are okay with the race of sending data and cancelling ... since this is "hot" by definition and unsubscribing is a race.
				o.onSubscribe(new Cancellation() {

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

	Worker clientThread = Schedulers.single().createWorker();
	Worker serverThread = Schedulers.single().createWorker();
	
	public void connectToServerConnection(TestConnection serverConnection, boolean log) {
		if (log) {
			serverConnection.write.add(n -> System.out.println("SERVER ==> Writes from server->client: " + n + "   Written from " + Thread.currentThread()));
			serverConnection.toInput.add(n -> System.out.println("SERVER <== Input from client->server: " + n + "   Read on " + Thread.currentThread()));
			write.add(n -> System.out.println("CLIENT ==> Writes from client->server: " + n + "   Written from " + Thread.currentThread()));
			toInput.add(n -> System.out.println("CLIENT <== Input from server->client: " + n + "   Read on " + Thread.currentThread()));
		}
		
		// client to server
		write.add(f -> {
//			serverConnection.toInput.send(f);
			serverThread.schedule(() -> {
				serverConnection.toInput.send(f);
			});
		});
		// server to client
		serverConnection.write.add(f -> {
//			toInput.send(f);
			clientThread.schedule(() -> {
				toInput.send(f);
			});
		});
	}

	@Override
	public void close() throws IOException {
		clientThread.shutdown();
		serverThread.shutdown();
	}

}