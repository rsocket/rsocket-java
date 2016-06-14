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

import io.reactivesocket.rx.Completable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler.Worker;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.util.logging.Level;

public class TestConnection implements DuplexConnection {

	public final DirectProcessor<Frame> toInput = DirectProcessor.create();
	public final DirectProcessor<Frame> write = DirectProcessor.create();

	@Override
	public void addOutput(Publisher<Frame> o, Completable callback) {
		Flux.from(o).flatMap(m -> {
			// no backpressure on a Subject so just firehosing for this test
			write.onNext(m);
			return Flux.empty();
		}).subscribe(v -> {
		} , callback::error, callback::success);
	}

	@Override
	public void addOutput(Frame f, Completable callback) {
        write.onNext(f);
        callback.success();
	}

	@Override
	public double availability() {
		return 1.0;
	}

	@Override
	public Publisher<Frame> getInput() {
		return toInput;
	}

	public void connectToServerConnection(TestConnection serverConnection) {
		connectToServerConnection(serverConnection, true);
	}

	Worker clientThread = Schedulers.single().createWorker();
	Worker serverThread = Schedulers.single().createWorker();
	
	public void connectToServerConnection(TestConnection serverConnection, boolean log) {
		/*if (log) {
			serverConnection.write.add(n -> System.out.println("SERVER ==> Writes from server->client: " + n + "   Written from " + Thread.currentThread()));
			serverConnection.toInput.add(n -> System.out.println("SERVER <== Input from client->server: " + n + "   Read on " + Thread.currentThread()));
			write.add(n -> System.out.println("CLIENT ==> Writes from client->server: " + n + "   Written from " + Thread.currentThread()));
			toInput.add(n -> System.out.println("CLIENT <== Input from server->client: " + n + "   Read on " + Thread.currentThread()));
		} */


		write
			.log(TestConnection.class.getName(), Level.ALL)
			.subscribe(serverConnection.toInput);

		serverConnection
			.write
			.log(TestConnection.class.getName(), Level.ALL)
			.subscribe(toInput);
	}

	@Override
	public void close() throws IOException {
		clientThread.shutdown();
		serverThread.shutdown();
	}

}