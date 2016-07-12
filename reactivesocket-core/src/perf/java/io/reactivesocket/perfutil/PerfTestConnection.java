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

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.internal.EmptySubject;
import io.reactivesocket.rx.Completable;
import io.reactivesocket.rx.Observable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class PerfTestConnection implements DuplexConnection {

	public final PerfUnicastSubjectNoBackpressure<Frame> toInput = PerfUnicastSubjectNoBackpressure.create();
	private PerfUnicastSubjectNoBackpressure<Frame> writeSubject = PerfUnicastSubjectNoBackpressure.create();
	private final EmptySubject closeSubject = new EmptySubject();

	@Override
	public void addOutput(Publisher<Frame> o, Completable callback) {
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
				callback.error(t);
			}

			@Override
			public void onComplete() {
				callback.success();
			}

		});
	}

	@Override
	public void addOutput(Frame f, Completable callback) {
		writeSubject.onNext(f);
		callback.success();
	}

	@Override
	public double availability() {
		return 1.0;
	}

	@Override
	public Observable<Frame> getInput() {
		return toInput;
	}

	public void connectToServerConnection(PerfTestConnection serverConnection) {
		writeSubject.subscribe(serverConnection.toInput);
		serverConnection.writeSubject.subscribe(toInput);
	}

	@Override
	public Publisher<Void> close() {
		return s -> {
			closeSubject.onComplete();
			closeSubject.subscribe(s);
		};
	}

	@Override
	public Publisher<Void> onClose() {
		return closeSubject;
	}
}