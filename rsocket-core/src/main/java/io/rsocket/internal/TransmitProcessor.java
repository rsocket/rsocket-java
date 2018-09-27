/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rsocket.internal;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

public final class TransmitProcessor<T>
		extends FluxProcessor<T, T>
		implements CoreSubscriber<T>, Subscription {

	/**
	 * Create a new {@link TransmitProcessor} that will buffer on an internal queue in an
	 * unbounded fashion.
	 *
	 * @param <E> the relayed type
	 * @return a unicast {@link FluxProcessor}
	 */
	public static <E> TransmitProcessor<E> create() {
		return new TransmitProcessor<>();
	}

	volatile Disposable                                                     onTerminate;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<TransmitProcessor, Disposable> ON_TERMINATE =
			AtomicReferenceFieldUpdater.newUpdater(TransmitProcessor.class, Disposable.class, "onTerminate");

	volatile boolean done;
	Throwable error;

	volatile CoreSubscriber<? super T> actual;

	volatile boolean cancelled;


	volatile long requested;
	@SuppressWarnings("rawtypes")
	static final AtomicLongFieldUpdater<TransmitProcessor> REQUESTED =
			AtomicLongFieldUpdater.newUpdater(TransmitProcessor.class, "requested");

	volatile int once;
	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<TransmitProcessor> ONCE =
			AtomicIntegerFieldUpdater.newUpdater(TransmitProcessor.class, "once");

	volatile Subscription upstream;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<TransmitProcessor, Subscription> UPSTREAM =
			AtomicReferenceFieldUpdater.newUpdater(TransmitProcessor.class, Subscription.class, "upstream");

	public TransmitProcessor() {
		this.onTerminate = null;
	}

	public TransmitProcessor(Disposable onTerminate) {
		this.onTerminate = Objects.requireNonNull(onTerminate, "onTerminate");
	}


	@Override
	public int getBufferSize() {
		return 0;
	}

	void doTerminate() {
		Disposable r = onTerminate;
		if (r != null && ON_TERMINATE.compareAndSet(this, r, null)) {
			r.dispose();
		}
	}

	@Override
	public void onSubscribe(Subscription s) {
		if (done || cancelled) {
			s.cancel();
		} else {
			if (UPSTREAM.compareAndSet(this,null, s)) {
				long r = REQUESTED.getAndSet(this, 0);
				if (r > 0) {
					s.request(r);
				}
			}
			else {
				s.cancel();
			}
		}
	}

	@Override
	public int getPrefetch() {
		return 0;
	}

	@Override
	public Context currentContext() {
		CoreSubscriber<? super T> actual = this.actual;
		return actual != null ? actual.currentContext() : Context.empty();
	}

	@Override
	public void onNext(T t) {
		if (done || cancelled) {
			Operators.onNextDropped(t, currentContext());
			return;
		}

		actual.onNext(t);
	}

	@Override
	public void onError(Throwable t) {
		if (done || cancelled) {
			Operators.onErrorDropped(t, currentContext());
			return;
		}

		error = t;
		done = true;

		doTerminate();

		if (actual != null) {
			actual.onError(t);
		}
	}

	@Override
	public void onComplete() {
		if (done || cancelled) {
			return;
		}

		done = true;

		doTerminate();

		if (actual != null) {
			actual.onComplete();
		}
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		Objects.requireNonNull(actual, "subscribe");
		if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
			if (!cancelled) {
				this.actual = actual;

				if (done) {
					actual.onSubscribe(Operators.emptySubscription());
					if (error == null) {
						actual.onComplete();
					}
					else {
						actual.onError(error);
					}
				}
				else {
					actual.onSubscribe(this);
				}
			}
			else {
				actual.onError(new RuntimeException("Cancelled"));
			}
		} else {
			Operators.error(actual, new IllegalStateException("TransmitProcessor " +
					"allows only a single Subscriber"));
		}
	}

	@Override
	public void request(long n) {
		if (upstream == null && Operators.validate(n)) {
			Operators.addCap(REQUESTED, this, n);
		}
		else {
			upstream.request(n);
		}
	}

	@Override
	public void cancel() {
		if (cancelled) {
			return;
		}
		cancelled = true;

		doTerminate();
	}

	@Override
	public boolean isDisposed() {
		return cancelled || done;
	}

	@Override
	public boolean isTerminated() {
		return done;
	}

	@Override
	@Nullable
	public Throwable getError() {
		return error;
	}

	public CoreSubscriber<? super T> actual() {
		return actual;
	}

	@Override
	public long downstreamCount() {
		return hasDownstreams() ? 1L : 0L;
	}

	@Override
	public boolean hasDownstreams() {
		return actual != null;
	}
}
