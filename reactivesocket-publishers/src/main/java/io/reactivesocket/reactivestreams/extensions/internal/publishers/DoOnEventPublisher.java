/*
 * Copyright 2016 Netflix, Inc.
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

package io.reactivesocket.reactivestreams.extensions.internal.publishers;

import io.reactivesocket.reactivestreams.extensions.Px;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

public final class DoOnEventPublisher<T> implements Px<T> {
    private final Runnable doOnCancel;
    private final Consumer<T> doOnNext;
    private final Consumer<Throwable> doOnError;
    private final Runnable doOnComplete;
    private final Consumer<Subscription> doOnSubscribe;
    private final LongConsumer doOnRequest;
    private final Publisher<T> source;

    private DoOnEventPublisher(Px<T> source,
                               Consumer<Subscription> doOnSubscribe,
                               Runnable doOnCancel,
                               Consumer<T> doOnNext,
                               Consumer<Throwable> doOnError,
                               Runnable doOnComplete,
                               LongConsumer doOnRequest) {
        Objects.requireNonNull(source, "source subscriber must not be null");
        this.source = source;
        this.doOnSubscribe = doOnSubscribe;
        this.doOnCancel = doOnCancel;
        this.doOnRequest = doOnRequest;
        this.doOnNext = doOnNext;
        this.doOnError = doOnError;
        this.doOnComplete = doOnComplete;
    }

    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
        source.subscribe(new Subscriber<T>() {
            @Override
            public void onSubscribe(Subscription s) {
                if (doOnSubscribe != null) {
                    doOnSubscribe.accept(s);
                }

                if (null == doOnRequest && null == doOnCancel) {
                    subscriber.onSubscribe(s);
                } else {
                    subscriber.onSubscribe(decorateSubscription(s));
                }
            }

            @Override
            public void onNext(T t) {
                if (doOnNext != null) {
                    doOnNext.accept(t);
                }
                subscriber.onNext(t);
            }

            @Override
            public void onError(Throwable t) {
                if (doOnError != null) {
                    doOnError.accept(t);
                }
                subscriber.onError(t);
            }

            @Override
            public void onComplete() {
                if (doOnComplete != null) {
                    doOnComplete.run();
                }
                subscriber.onComplete();
            }
        });
    }

    private Subscription decorateSubscription(Subscription subscription) {
        return new Subscription() {
            @Override
            public void request(long n) {
                if (doOnRequest != null) {
                    doOnRequest.accept(n);
                }
                subscription.request(n);
            }

            @Override
            public void cancel() {
                if (doOnCancel != null) {
                    doOnCancel.run();
                }
                subscription.cancel();
            }
        };
    }

    public static <T> Px<T> onNext(Px<T> source, Consumer<T> doOnNext) {
        return new DoOnEventPublisher<>(source, null, null, doOnNext::accept, null, null, null);
    }

    public static <T> Px<T> onError(Px<T> source, Consumer<Throwable> doOnError) {
        return new DoOnEventPublisher<>(source, null, null, null, doOnError, null, null);
    }

    public static <T> Px<T> onComplete(Px<T> source, Runnable doOnComplete) {
        return new DoOnEventPublisher<>(source, null, null, null, null, doOnComplete, null);
    }

    public static <T> Px<T> onCompleteOrError(Px<T> source, Runnable doOnComplete, Consumer<Throwable> doOnError) {
        return new DoOnEventPublisher<>(source, null, null, null, doOnError, doOnComplete, null);
    }

    public static <T> Px<T> onTerminate(Px<T> source, Runnable doOnTerminate) {
        return new DoOnEventPublisher<>(source, null, null, null, throwable -> doOnTerminate.run(),
                                         doOnTerminate, null);
    }

    public static <T> Px<T> onCompleteOrErrorOrCancel(Px<T> source, Runnable doOnCancel, Runnable doOnComplete, Consumer<Throwable> doOnError) {
        return new DoOnEventPublisher<>(source, null, doOnCancel, null, doOnError,
            doOnComplete, null);
    }

    public static <T> Px<T> onSubscribe(Px<T> source, Consumer<Subscription> doOnSubscribe) {
        return new DoOnEventPublisher<T>(source, doOnSubscribe, null, null, null, null, null);
    }

    public static <T> Px<T> onCancel(Px<T> source, Runnable doOnCancel) {
        return new DoOnEventPublisher<>(source, null, doOnCancel, null, null, null, null);
    }

    public static <T> Px<T> onRequest(Px<T> source, LongConsumer doOnRequest) {
        return new DoOnEventPublisher<>(source, null, null, null, null, null, doOnRequest);
    }
}
