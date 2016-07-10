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

package io.reactivesocket.internal;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.function.Consumer;

import static io.reactivesocket.internal.CancellableSubscriberImpl.*;

/**
 * A factory to create instances of {@link Subscriber} that follow reactive stream specifications.
 */
public final class Subscribers {

    private Subscribers() {
    }

    /**
     * Creates a new {@code Subscriber} instance that ignores all invocations but follows reactive streams specfication.
     *
     * @param <T> Type parameter
     *
     * @return A new {@code Subscriber} instance.
     */
    public static <T> CancellableSubscriber<T> empty() {
        return new CancellableSubscriberImpl<T>();
    }

    /**
     * Creates a new {@code Subscriber} instance that ignores all invocations other than
     * {@link Subscriber#onSubscribe(Subscription)} but follows reactive streams specfication.
     *
     * @param doOnSubscribe Callback for {@link Subscriber#onSubscribe(Subscription)}
     * @param <T> Type parameter
     *
     * @return A new {@code Subscriber} instance.
     */
    public static <T> CancellableSubscriber<T> doOnSubscribe(Consumer<Subscription> doOnSubscribe) {
        return new CancellableSubscriberImpl<T>(doOnSubscribe, EMPTY_RUNNABLE, emptyOnNext(), EMPTY_ON_ERROR,
                                                EMPTY_RUNNABLE);
    }

    /**
     * Creates a new {@code Subscriber} instance that ignores all invocations other than
     * {@link CancellableSubscriber#cancel()} but follows reactive streams specfication.
     *
     * @param doOnCancel Callback for {@link CancellableSubscriber#cancel()}
     * @param <T> Type parameter
     *
     * @return A new {@code Subscriber} instance.
     */
    public static <T> CancellableSubscriber<T> doOnCancel(Runnable doOnCancel) {
        return new CancellableSubscriberImpl<T>(EMPTY_ON_SUBSCRIBE, doOnCancel, emptyOnNext(), EMPTY_ON_ERROR,
                                                EMPTY_RUNNABLE);
    }

    /**
     * Creates a new {@code Subscriber} instance that ignores all invocations other than
     * {@link Subscriber#onSubscribe(Subscription)} and {@link CancellableSubscriber#cancel()} but follows reactive
     * streams specfication.
     *
     * @param doOnSubscribe Callback for {@link Subscriber#onSubscribe(Subscription)}
     * @param doOnCancel Callback for {@link CancellableSubscriber#cancel()}
     * @param <T> Type parameter
     *
     * @return A new {@code Subscriber} instance.
     */
    public static <T> CancellableSubscriber<T> create(Consumer<Subscription> doOnSubscribe, Runnable doOnCancel) {
        return new CancellableSubscriberImpl<T>(doOnSubscribe, doOnCancel, emptyOnNext(), EMPTY_ON_ERROR,
                                                EMPTY_RUNNABLE);
    }

    /**
     * Creates a new {@code Subscriber} instance that listens to callbacks for all methods and follows reactive streams
     * specfication.
     *
     * @param doOnSubscribe Callback for {@link Subscriber#onSubscribe(Subscription)}
     * @param doOnNext Callback for {@link Subscriber#onNext(Object)}
     * @param doOnError Callback for {@link Subscriber#onError(Throwable)}
     * @param doOnComplete Callback for {@link Subscriber#onComplete()}
     * @param doOnCancel Callback for {@link CancellableSubscriber#cancel()}
     * @param <T> Type parameter
     *
     * @return A new {@code Subscriber} instance.
     */
    public static <T> CancellableSubscriber<T> create(Consumer<Subscription> doOnSubscribe, Consumer<T> doOnNext,
                                                      Consumer<Throwable> doOnError, Runnable doOnComplete,
                                                      Runnable doOnCancel) {
        return new CancellableSubscriberImpl<T>(doOnSubscribe, doOnCancel, doOnNext, doOnError, doOnComplete);
    }

    /**
     * Creates a new {@code Subscriber} instance that ignores all invocations other than
     * {@link Subscriber#onError(Throwable)} but follows reactive streams specfication.
     *
     * @param doOnError Callback for {@link Subscriber#onError(Throwable)}
     * @param <T> Type parameter
     *
     * @return A new {@code Subscriber} instance.
     */
    public static <T> CancellableSubscriber<T> doOnError(Consumer<Throwable> doOnError) {
        return new CancellableSubscriberImpl<T>(EMPTY_ON_SUBSCRIBE, EMPTY_RUNNABLE, emptyOnNext(), doOnError,
                                                EMPTY_RUNNABLE);
    }

    /**
     * Creates a new {@code Subscriber} instance that ignores all invocations other than
     * {@link Subscriber#onComplete()}, {@link Subscriber#onError(Throwable)} but follows reactive streams specfication.
     *
     * @param doOnComplete Callback for {@link Subscriber#onComplete()}
     * @param doOnError Callback for {@link Subscriber#onError(Throwable)}
     * @param <T> Type parameter
     *
     * @return A new {@code Subscriber} instance.
     */
    public static <T> CancellableSubscriber<T> doOnComplete(Runnable doOnComplete, Consumer<Throwable> doOnError) {
        return new CancellableSubscriberImpl<T>(EMPTY_ON_SUBSCRIBE, EMPTY_RUNNABLE, emptyOnNext(), doOnError,
                                                doOnComplete);
    }

    /**
     * Creates a new {@code Subscriber} instance that ignores all invocations other than
     * {@link Subscriber#onNext(Object)} and {@link Subscriber#onError(Throwable)}but follows reactive streams
     * specfication.
     *
     * @param doOnNext Callback for {@link Subscriber#onNext(Object)}
     * @param doOnError Callback for {@link Subscriber#onError(Throwable)}
     * @param <T> Type parameter
     *
     * @return A new {@code Subscriber} instance.
     */
    public static <T> CancellableSubscriber<T> doOnNext(Consumer<T> doOnNext, Consumer<Throwable> doOnError) {
        return new CancellableSubscriberImpl<T>(EMPTY_ON_SUBSCRIBE, EMPTY_RUNNABLE, doOnNext, doOnError,
                                                EMPTY_RUNNABLE);
    }

    /**
     * Creates a new {@code Subscriber} instance that ignores all invocations other than
     * {@link Subscriber#onSubscribe(Subscription)}, {@link Subscriber#onNext(Object)} and
     * {@link Subscriber#onError(Throwable)} but follows reactive streams specfication.
     *
     * @param doOnSubscribe Callback for {@link Subscriber#onSubscribe(Subscription)}
     * @param doOnNext Callback for {@link Subscriber#onNext(Object)}
     * @param doOnError Callback for {@link Subscriber#onError(Throwable)}
     * @param <T> Type parameter
     *
     * @return A new {@code Subscriber} instance.
     */
    public static <T> CancellableSubscriber<T> doOnNext(Consumer<Subscription> doOnSubscribe, Consumer<T> doOnNext,
                                                        Consumer<Throwable> doOnError) {
        return new CancellableSubscriberImpl<T>(doOnSubscribe, EMPTY_RUNNABLE, doOnNext, doOnError, EMPTY_RUNNABLE);
    }

    /**
     * Creates a new {@code Subscriber} instance that ignores all invocations other than
     * {@link Subscriber#onSubscribe(Subscription)}, {@link Subscriber#onNext(Object)},
     * {@link Subscriber#onError(Throwable)} and {@link Subscriber#onComplete()} but follows reactive streams
     * specfication.
     *
     * @param doOnSubscribe Callback for {@link Subscriber#onSubscribe(Subscription)}
     * @param doOnNext Callback for {@link Subscriber#onNext(Object)}
     * @param doOnError Callback for {@link Subscriber#onError(Throwable)}
     * @param doOnComplete Callback for {@link Subscriber#onComplete()}
     * @param <T> Type parameter
     *
     * @return A new {@code Subscriber} instance.
     */
    public static <T> CancellableSubscriber<T> doOnNext(Consumer<Subscription> doOnSubscribe, Consumer<T> doOnNext,
                                                        Consumer<Throwable> doOnError, Runnable doOnComplete) {
        return new CancellableSubscriberImpl<T>(doOnSubscribe, EMPTY_RUNNABLE, doOnNext, doOnError, doOnComplete);
    }

    /**
     * Creates a new {@code Subscriber} instance that ignores all invocations other than
     * {@link Subscriber#onError(Throwable)} and {@link Subscriber#onComplete()} but follows reactive streams
     * specfication.
     *
     * @param doOnError Callback for {@link Subscriber#onError(Throwable)}
     * @param doOnComplete Callback for {@link Subscriber#onComplete()}
     * @param <T> Type parameter
     *
     * @return A new {@code Subscriber} instance.
     */
    public static <T> CancellableSubscriber<T> doOnTerminate(Consumer<Throwable> doOnError, Runnable doOnComplete) {
        return new CancellableSubscriberImpl<T>(EMPTY_ON_SUBSCRIBE, EMPTY_RUNNABLE, emptyOnNext(), doOnError,
                                                doOnComplete);
    }
}
