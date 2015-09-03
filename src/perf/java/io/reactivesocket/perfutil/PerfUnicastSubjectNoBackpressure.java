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

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * The difference between this and the real UnicastSubject is in the `onSubscribe` method where it calls requestN. Not sure that behavior should exist in the producton code. 
 */
public final class PerfUnicastSubjectNoBackpressure<T> implements Subscriber<T>, Publisher<T> {

	private Subscriber<? super T> s;
	private final BiConsumer<PerfUnicastSubjectNoBackpressure<T>, Long> onConnect;
	private boolean subscribedTo = false;

	public static <T> PerfUnicastSubjectNoBackpressure<T> create() {
		return new PerfUnicastSubjectNoBackpressure<T>(null);
	}

	/**
	 * @param onConnect Called when first requestN > 0 occurs.
	 * @return
	 */
	public static <T> PerfUnicastSubjectNoBackpressure<T> create(BiConsumer<PerfUnicastSubjectNoBackpressure<T>, Long> onConnect) {
		return new PerfUnicastSubjectNoBackpressure<T>(onConnect);
	}

	private PerfUnicastSubjectNoBackpressure(BiConsumer<PerfUnicastSubjectNoBackpressure<T>, Long> onConnect) {
		this.onConnect = onConnect;
	}

	@Override
	public void onSubscribe(Subscription s) {
		s.request(Long.MAX_VALUE); // This does not feed ReqeustN through
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
	public void subscribe(Subscriber<? super T> s) {
		if (this.s != null) {
			s.onError(new IllegalStateException("Only single Subscriber supported"));
		} else {
			this.s = s;
			this.s.onSubscribe(new Subscription() {

				boolean started = false;

				@Override
				public void request(long n) {
					if (n > 0) {
						if (!started) {
							started = true;
							subscribedTo = true;
							// now actually connected
							if (onConnect != null) {
								onConnect.accept(PerfUnicastSubjectNoBackpressure.this, n);
							}
						}
					}
				}

				@Override
				public void cancel() {
					// transport has shut us down
				}

			});
		}
	}
	
	public boolean isSubscribedTo() {
		return subscribedTo;
	}

}
