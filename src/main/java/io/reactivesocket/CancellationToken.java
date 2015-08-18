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
import rx.Subscriber;

/**
 * Intended to ONLY support a single Subscriber and Publisher for notification of a cancellation event, such as with takeUntil.
 */
/* package */ class CancellationToken extends Observable<Void> {

	public static CancellationToken create() {
		return new CancellationToken(new State());
	}

	private State state;

	protected CancellationToken(State state) {
		super(s -> {
			synchronized (state) {
				if (state.cancelled) {
					s.onCompleted(); // always onComplete when cancelled
				} else {
					if (state.subscriber != null) {
						throw new IllegalStateException("Only 1 Subscriber permitted on a CancellationSubject");
					} else {
						state.subscriber = s;
					}
				}
			}
		});
		this.state = state;
	}

	public final void cancel() {
		Subscriber<?> emitTo;
		synchronized (state) {
			state.cancelled = true;
			emitTo = state.subscriber;
		}
		if (emitTo != null) {
			emitTo.onCompleted();
		}
	}

	private static class State {
		private Subscriber<?> subscriber;
		private boolean cancelled = false;
	}
}
