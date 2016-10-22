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

package io.reactivesocket.reactivestreams.extensions.internal.subscribers;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.function.Consumer;

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
        return new CancellableSubscriberImpl<>(doOnSubscribe, null, null, null,
                                                null);
    }

    /**
     * Creates a new {@code Subscriber} instance that ignores all invocations other than
     * {@link Subscriber#onSubscribe(Subscription)} and {@link Subscription#cancel()} but follows reactive
     * streams specfication.
     *
     * @param doOnSubscribe Callback for {@link Subscriber#onSubscribe(Subscription)}
     * @param doOnCancel Callback for {@link Subscription#cancel()}
     * @param <T> Type parameter
     *
     * @return A new {@code Subscriber} instance.
     */
    public static <T> CancellableSubscriber<T> create(Consumer<Subscription> doOnSubscribe, Runnable doOnCancel) {
        return new CancellableSubscriberImpl<T>(doOnSubscribe, doOnCancel, null, null,
                                                null);
    }

    /**
     * Creates a new {@code Subscriber} instance that listens to callbacks for all methods and follows reactive streams
     * specfication.
     *
     * @param doOnSubscribe Callback for {@link Subscriber#onSubscribe(Subscription)}
     * @param doOnNext Callback for {@link Subscriber#onNext(Object)}
     * @param doOnError Callback for {@link Subscriber#onError(Throwable)}
     * @param doOnComplete Callback for {@link Subscriber#onComplete()}
     * @param doOnCancel Callback for {@link Subscription#cancel()}
     * @param <T> Type parameter
     *
     * @return A new {@code Subscriber} instance.
     */
    public static <T> CancellableSubscriber<T> create(Consumer<Subscription> doOnSubscribe, Consumer<T> doOnNext,
                                                      Consumer<Throwable> doOnError, Runnable doOnComplete,
                                                      Runnable doOnCancel) {
        return new CancellableSubscriberImpl<>(doOnSubscribe, doOnCancel, doOnNext, doOnError, doOnComplete);
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
        return new CancellableSubscriberImpl<T>(null, null, null, doOnError,
                                                null);
    }

    /**
     * Creates a new {@code Subscriber} instance that ignores all invocations other than
     * {@link Subscriber#onError(Throwable)} and {@link Subscriber#onComplete()} but follows reactive streams
     * specfication.
     *
     * @param doOnTerminate Callback after the source finished with error or success.
     * @param <T> Type parameter
     *
     * @return A new {@code Subscriber} instance.
     */
    public static <T> CancellableSubscriber<T> doOnTerminate(Runnable doOnTerminate) {
        return new CancellableSubscriberImpl<T>(null, null, null,
                                                throwable -> doOnTerminate.run(), doOnTerminate);
    }

    /**
     * Creates a new {@code Subscriber} instance that ignores all invocations other than
     * {@link Subscriber#onError(Throwable)}, {@link Subscriber#onComplete()} and
     * {@link Subscription#cancel()} but follows reactive streams specfication.
     *
     * @param doFinally Callback invoked exactly once, after the source finished with error, success or cancelled.
     * @param <T> Type parameter
     *
     * @return A new {@code Subscriber} instance.
     */
    public static <T> CancellableSubscriber<? super T> cleanup(Runnable doFinally) {
        Runnable runOnce = new Runnable() {
            private boolean done;

            @Override
            public void run() {
                synchronized (this) {
                    if (done) {
                        return;
                    }
                    done = true;
                }
                doFinally.run();
            }
        };

        return new CancellableSubscriberImpl<T>(null, runOnce, null,
                                                throwable -> runOnce.run(), runOnce);
    }
}
