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
package io.reactivesocket.internal;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Intended to ONLY support a single Subscriber. It will throw an exception if more than 1 subscribe occurs.
 * <p>
 * This differs from PublishSubject which allows multicasting. This is done for efficiency reasons.
 * <p>
 * This is NOT thread-safe.
 */
public final class UnicastSubject<T> implements Subscriber<T>, Publisher<T> {

	private Subscriber<? super T> s;
	private final BiConsumer<UnicastSubject<T>, Long> onConnect;
	private final Consumer<Long> onRequest;
	private boolean subscribedTo = false;

	public static <T> UnicastSubject<T> create() {
		return new UnicastSubject<>(null, r -> {});
	}

	/**
	 * @param onConnect Called when first requestN > 0 occurs.
	 * @param onRequest Called for each requestN after the first one (which invokes onConnect)
	 * @return
	 */
	public static <T> UnicastSubject<T> create(BiConsumer<UnicastSubject<T>, Long> onConnect, Consumer<Long> onRequest) {
		return new UnicastSubject<>(onConnect, onRequest);
	}
	
	/**
	 * @param onConnect Called when first requestN > 0 occurs.
	 * @return
	 */
	public static <T> UnicastSubject<T> create(BiConsumer<UnicastSubject<T>, Long> onConnect) {
		return new UnicastSubject<>(onConnect,  r -> {});
	}

	private UnicastSubject(BiConsumer<UnicastSubject<T>, Long> onConnect, Consumer<Long> onRequest) {
		this.onConnect = onConnect;
		this.onRequest = onRequest;
	}

	@Override
	public void onSubscribe(Subscription s) {
		throw new IllegalStateException("This UnicastSubject does not support being used as a Subscriber to a Publisher");
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
								onConnect.accept(UnicastSubject.this, n);
							}
						} else {
							onRequest.accept(n);
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
