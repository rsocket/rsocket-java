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

import java.util.function.Consumer;

import io.reactivesocket.observable.Disposable;
import io.reactivesocket.observable.Observable;
import io.reactivesocket.observable.Observer;

/**
 * The difference between this and the real UnicastSubject is in the `onSubscribe` method where it calls requestN. Not sure that behavior should exist in the producton code. 
 */
public final class PerfUnicastSubjectNoBackpressure<T> implements Observable<T>, Observer<T> {

	private Observer<? super T> s;
	private final Consumer<PerfUnicastSubjectNoBackpressure<T>> onConnect;
	private boolean subscribedTo = false;

	public static <T> PerfUnicastSubjectNoBackpressure<T> create() {
		return new PerfUnicastSubjectNoBackpressure<T>(null);
	}

	/**
	 * @param onConnect Called when first requestN > 0 occurs.
	 * @return
	 */
	public static <T> PerfUnicastSubjectNoBackpressure<T> create(Consumer<PerfUnicastSubjectNoBackpressure<T>> onConnect) {
		return new PerfUnicastSubjectNoBackpressure<T>(onConnect);
	}

	private PerfUnicastSubjectNoBackpressure(Consumer<PerfUnicastSubjectNoBackpressure<T>> onConnect) {
		this.onConnect = onConnect;
	}

	@Override
	public void onSubscribe(Disposable s) {
	}

	@Override
	public void onNext(T t) {
		s.onNext(t);
	}

	@Override
	public void onError(Throwable t) {
		s.onError(t);
	}

	@Override
	public void onComplete() {
		s.onComplete();
	}

	@Override
	public void subscribe(Observer<T> s) {
		if (this.s != null) {
			s.onError(new IllegalStateException("Only single Subscriber supported"));
		} else {
			this.s = s;
			this.s.onSubscribe(new Disposable() {

				@Override
				public void dispose() {
					// transport has shut us down
				}

			});
			if(onConnect != null) {
				onConnect.accept(PerfUnicastSubjectNoBackpressure.this);
			}
		}
	}
	
	public boolean isSubscribedTo() {
		return subscribedTo;
	}

}
