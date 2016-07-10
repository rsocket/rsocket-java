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

import org.reactivestreams.Subscription;

import java.util.function.Consumer;

final class CancellableSubscriberImpl<T> implements CancellableSubscriber<T> {

    static final Consumer<Subscription> EMPTY_ON_SUBSCRIBE = new Consumer<Subscription>() {
        @Override
        public void accept(Subscription subscription) {
            // No Op; empty
        }
    };

    static final Consumer<Throwable> EMPTY_ON_ERROR = new Consumer<Throwable>() {
        @Override
        public void accept(Throwable throwable) {
            // No Op; empty
        }
    };

    static final Runnable EMPTY_RUNNABLE = new Runnable() {
        @Override
        public void run() {
            // No Op; empty
        }
    };

    private final Runnable onCancel;
    private final Consumer<T> doOnNext;
    private final Consumer<Throwable> doOnError;
    private final Runnable doOnComplete;
    private final Consumer<Subscription> doOnSubscribe;
    private Subscription s;
    private boolean done;
    private boolean cancelled;
    private boolean subscribed;

    public CancellableSubscriberImpl(Consumer<Subscription> doOnSubscribe, Runnable doOnCancel, Consumer<T> doOnNext,
                                     Consumer<Throwable> doOnError, Runnable doOnComplete) {
        this.doOnSubscribe = doOnSubscribe;
        onCancel = doOnCancel;
        this.doOnNext = doOnNext;
        this.doOnError = doOnError;
        this.doOnComplete = doOnComplete;
    }

    public CancellableSubscriberImpl() {
        this(EMPTY_ON_SUBSCRIBE, EMPTY_RUNNABLE, t -> {}, EMPTY_ON_ERROR, EMPTY_RUNNABLE);
    }

    @Override
    public void onSubscribe(Subscription s) {

        boolean _cancel = false;
        boolean _subscribed;
        synchronized (this) {
            _subscribed = subscribed;
            if (!subscribed) {
                subscribed = true;
                this.s = s;
                if (cancelled) {
                    _cancel = true;
                }
            }
        }

        if (_subscribed) {
            onError(new IllegalStateException("Duplicate subscription."));
        } else if (_cancel) {
            _unsafeCancel();
        } else {
            doOnSubscribe.accept(s);
        }
    }

    @Override
    public void cancel() {
        boolean _cancel = false;
        synchronized (this) {
            if (s != null && !cancelled) {
                _cancel = true;
            }
            cancelled = true;
            done = true;
        }

        if (_cancel) {
            _unsafeCancel();
        }
    }

    @Override
    public void onNext(T t) {
        if (canEmit()) {
            doOnNext.accept(t);
        }
    }

    @Override
    public void onError(Throwable t) {
        if (!terminate()) {
            doOnError.accept(t);
        }
    }

    @Override
    public void onComplete() {
        if (!terminate()) {
            doOnComplete.run();
        }
    }

    static <T> Consumer<T> emptyOnNext() {
        return t -> {};
    }

    private synchronized boolean terminate() {
        boolean oldDone = done;
        done = true;
        return oldDone;
    }

    private synchronized boolean canEmit() {
        return !done;
    }

    private void _unsafeCancel() {
        s.cancel();
        onCancel.run();
    }
}
