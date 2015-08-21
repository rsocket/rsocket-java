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
package io.reactivesocket.perfutil;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.internal.UnicastSubject;

public class PerfTestConnection implements DuplexConnection {

	public final UnicastSubject toInput = UnicastSubject.create();
	private UnicastSubject writeSubject = UnicastSubject.create();
	public final Publisher<Frame> writes = writeSubject;

	@Override
	public Publisher<Void> addOutput(Publisher<Frame> o) {
		return new Publisher<Void>() {

			@Override
			public void subscribe(Subscriber<? super Void> child) {
				child.onSubscribe(new Subscription() {

					boolean started = false;

					@Override
					public void request(long n) {
						if (!started) {
							started = true;
							o.subscribe(new Subscriber<Frame>() {

								@Override
								public void onSubscribe(Subscription s) {
									s.request(Long.MAX_VALUE);
								}

								@Override
								public void onNext(Frame f) {
									writeSubject.onNext(f);
								}

								@Override
								public void onError(Throwable t) {
									child.onError(t);
								}

								@Override
								public void onComplete() {
									// do nothing
								}

							});
						}

					}

					@Override
					public void cancel() {
						// TODO not doing anything with this
					}

				});

			}

		};

	}

	@Override
	public Publisher<Frame> getInput() {
		return toInput;
	}

	public void connectToServerConnection(PerfTestConnection serverConnection) {
		writes.subscribe(serverConnection.toInput);
		serverConnection.writes.subscribe(toInput);

	}
}