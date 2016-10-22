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

import org.reactivestreams.Subscription;

import java.util.function.Consumer;

public final class CancellableSubscriberImpl<T> implements CancellableSubscriber<T> {

    private final Runnable onCancel;
    private final Consumer<T> doOnNext;
    private final Consumer<Throwable> doOnError;
    private final Runnable doOnComplete;
    private final Consumer<Subscription> doOnSubscribe;
    private Subscription subscription;
    private boolean done;
    private boolean cancelled;
    private boolean subscribed;

    public CancellableSubscriberImpl(
        Consumer<Subscription> doOnSubscribe,
        Runnable doOnCancel,
        Consumer<T> doOnNext,
        Consumer<Throwable> doOnError,
        Runnable doOnComplete) {
        this.doOnSubscribe = doOnSubscribe;
        onCancel = doOnCancel;
        this.doOnNext = doOnNext;
        this.doOnError = doOnError;
        this.doOnComplete = doOnComplete;
    }

    @SuppressWarnings("unchecked")
    public CancellableSubscriberImpl() {
        this(null, null, null, null, null);
    }

    @Override
    public void request(long n) {
        subscription.request(n);
    }

    @Override
    public void onSubscribe(Subscription s) {
        boolean _cancel = false;
        synchronized (this) {
            if (!subscribed) {
                subscribed = true;
                this.subscription = s;
                if (cancelled) {
                    _cancel = true;
                }
            } else {
                _cancel = true;
            }
        }

        if (_cancel) {
            s.cancel();
        } else  if (doOnSubscribe != null) {
            doOnSubscribe.accept(s);
        }
    }

    @Override
    public void cancel() {
        boolean _cancel = false;
        synchronized (this) {
            if (subscription != null && !cancelled) {
                _cancel = true;
            }
            cancelled = true;
            done = true;
        }

        if (_cancel) {
            unsafeCancel();
        }
    }

    @Override
    public synchronized boolean isCancelled() {
        return cancelled;
    }

    @Override
    public void onNext(T t) {
        if (doOnNext != null && canEmit()) {
            doOnNext.accept(t);
        }
    }

    @Override
    public void onError(Throwable t) {
        if (doOnError != null && !terminate()) {
            doOnError.accept(t);
        }
    }

    @Override
    public void onComplete() {
        if (doOnComplete != null && !terminate()) {
            doOnComplete.run();
        }
    }

    private synchronized boolean terminate() {
        boolean oldDone = done;
        done = true;
        return oldDone;
    }

    private synchronized boolean canEmit() {
        return !done;
    }

    private void unsafeCancel() {
        subscription.cancel();
        if (onCancel != null) {
            onCancel.run();
        }
    }
}
