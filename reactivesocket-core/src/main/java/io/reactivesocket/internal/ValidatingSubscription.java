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
package io.reactivesocket.internal;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.function.LongConsumer;

public final class ValidatingSubscription<T> implements Subscription {

    private static final Subscription emptySubscription = new Subscription() {
        @Override
        public void request(long n) {
        }

        @Override
        public void cancel() {
        }
    };

    private enum State {
        Active, Cancelled, Done
    }

    private final Subscriber<? super T> subscriber;
    private final Runnable onCancel;
    private final LongConsumer onRequestN;

    private State state = State.Active; // protected by this

    private ValidatingSubscription(Subscriber<? super T> subscriber, Runnable onCancel, LongConsumer onRequestN) {
        this.subscriber = subscriber;
        this.onCancel = onCancel;
        this.onRequestN = onRequestN;
    }

    @Override
    public void request(long n) {
        final State currentState;

        synchronized (this) {
            currentState = state;
        }

        if (currentState == State.Active) {
            if (n <= 0) {
                // Since we already checked that the subscriber isn't complete.
                subscriber.onError(new IllegalArgumentException("Rule 3.9: n > 0 is required, but it was " + n));
            } else if (onRequestN != null) {
                onRequestN.accept(n);
            }
        }
    }

    @Override
    public void cancel() {
        synchronized (this) {
            if (state != State.Active) {
                return;
            }
            state = State.Cancelled;
        }

        if (onCancel != null) {
            onCancel.run();
        }
    }

    public Subscriber<? super T> getSubscriber() {
        return subscriber;
    }


    public void safeOnNext(T item) {
        synchronized (this) {
            if (state != State.Active) {
                return;
            }
        }
        subscriber.onNext(item);
    }

    public void safeOnComplete() {
        synchronized (this) {
            if (state != State.Active) {
                return;
            }
            state = State.Done;
        }
        subscriber.onComplete();
    }

    public void safeOnError(Throwable throwable) {
        synchronized (this) {
            if (state != State.Active) {
                return;
            }
            state = State.Done;
        }

        subscriber.onError(throwable);
    }

    public synchronized boolean isActive() {
        return state == State.Active;
    }

    public static <T> ValidatingSubscription<T> empty(Subscriber<? super T> subscriber) {
        return new ValidatingSubscription<>(subscriber, null, null);
    }

    public static <T> ValidatingSubscription<T> onCancel(Subscriber<? super T> subscriber, Runnable onCancel) {
        return new ValidatingSubscription<>(subscriber, onCancel, null);
    }

    public static <T> ValidatingSubscription<T> onRequestN(Subscriber<? super T> subscriber,
                                                           LongConsumer onRequestN) {
        return new ValidatingSubscription<>(subscriber, null, onRequestN);
    }

    public static <T> ValidatingSubscription<T> create(Subscriber<? super T> subscriber, Runnable onCancel,
                                                       LongConsumer onRequestN) {
        return new ValidatingSubscription<>(subscriber, onCancel, onRequestN);
    }

    public static Subscription empty() {
        return emptySubscription;
    }
}
