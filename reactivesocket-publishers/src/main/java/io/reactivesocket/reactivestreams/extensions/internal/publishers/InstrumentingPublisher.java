/*
 * Copyright 2016 Netflix, Inc.
 * <p>
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *  <p>
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  <p>
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations under the License.
 */

package io.reactivesocket.reactivestreams.extensions.internal.publishers;

import io.reactivesocket.reactivestreams.extensions.Px;
import io.reactivesocket.reactivestreams.extensions.Scheduler;
import io.reactivesocket.reactivestreams.extensions.internal.subscribers.CancellableSubscriber;
import io.reactivesocket.reactivestreams.extensions.internal.subscribers.Subscribers;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A {@link Px} instance that facilitates usecases that involve creating a state on subscription and using it for all
 * subsequent callbacks. eg: Capturing timings from subscription to termination.
 *
 * @param <T> Type of objects emitted by the publisher.
 * @param <X> State to be created on subscription.
 */
public final class InstrumentingPublisher<T, X> implements Px<T> {

    private final Publisher<T> source;
    private final Function<Subscriber<? super T>, X> stateFactory;
    private final BiConsumer<X, Throwable> onError;
    private final Consumer<X> onComplete;
    private final Consumer<X> onCancel;
    private final BiConsumer<X, T> onNext;

    public InstrumentingPublisher(Publisher<T> source, Function<Subscriber<? super T>, X> stateFactory,
                                  BiConsumer<X, Throwable> onError, Consumer<X> onComplete, Consumer<X> onCancel,
                                  BiConsumer<X, T> onNext) {
        this.source = source;
        this.stateFactory = stateFactory;
        this.onError = onError;
        this.onComplete = onComplete;
        this.onCancel = onCancel;
        this.onNext = onNext;
    }

    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
        source.subscribe(new Subscriber<T>() {

            private volatile X state;
            private volatile boolean emit = true;

            @Override
            public void onSubscribe(Subscription s) {
                state = stateFactory.apply(subscriber);
                if (null != onCancel) {
                    subscriber.onSubscribe(new Subscription() {
                        @Override
                        public void request(long n) {
                            s.request(n);
                        }

                        @Override
                        public void cancel() {
                            if (emit) {
                                onCancel.accept(state);
                            }
                            emit = false;
                            s.cancel();
                        }
                    });
                } else {
                    subscriber.onSubscribe(s);
                }
            }

            @Override
            public void onNext(T t) {
                if (emit && null != onNext) {
                    onNext.accept(state, t);
                }
                subscriber.onNext(t);
            }

            @Override
            public void onError(Throwable t) {
                if (emit && null != onError) {
                    onError.accept(state, t);
                }
                emit = false;
                subscriber.onError(t);
            }

            @Override
            public void onComplete() {
                if (emit && null != onComplete) {
                    onComplete.accept(state);
                }
                emit = false;
                subscriber.onComplete();
            }
        });
    }
}
