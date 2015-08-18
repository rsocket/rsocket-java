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

import rx.Observable;
import rx.Observer;
import rx.Subscriber;

/**
 * Intended to ONLY support a single Subscriber. It will throw an exception if more than 1 subscribe occurs.
 * <p>
 * This differs from PublishSubject which allows multicasting. This is done for efficiency reasons.
 * <p>
 * This is NOT thread-safe.
 */
final class UnicastSubject extends Observable<Frame>implements Observer<Frame> {

	public static UnicastSubject create() {
		return new UnicastSubject(new State());
	}

	private State state;

	protected UnicastSubject(State state) {
		super(s -> {
			System.out.println("state: " + state.subscriber);
			if (state.subscriber != null) {
				s.onError(new IllegalStateException("Only 1 Subscriber permitted on a CancellationSubject"));
			} else {
				state.subscriber = s;
			}
		});
		this.state = state;
	}

	@Override
	public void onCompleted() {
		state.subscriber.onCompleted();
	}

	@Override
	public void onError(Throwable e) {
		state.subscriber.onError(e);
	}

	@Override
	public void onNext(Frame t) {
		state.subscriber.onNext(t);
	}

	private static class State {
		private Subscriber<? super Frame> subscriber;
	}
}
