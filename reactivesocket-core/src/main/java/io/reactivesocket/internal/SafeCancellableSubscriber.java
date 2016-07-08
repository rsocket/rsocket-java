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

import java.util.concurrent.atomic.AtomicBoolean;

abstract class SafeCancellableSubscriber<T> extends CancellableSubscriber<T> {

    protected final AtomicBoolean subscribed = new AtomicBoolean();
    protected final AtomicBoolean done = new AtomicBoolean();

    @Override
    public void onSubscribe(Subscription s) {
        if (subscribed.compareAndSet(false, true)) {
            super.onSubscribe(s);
        } else {
            onError(new IllegalStateException("Duplicate subscription."));
        }
    }

    @Override
    public void onNext(T t) {
        if (!done.get()) {
            doOnNext(t);
        }
    }

    @Override
    public void onError(Throwable t) {
        if (done.compareAndSet(false, true)) {
            doOnError(t);
            super.cancel();
        }
    }

    @Override
    public void onComplete() {
        if (done.compareAndSet(false, true)) {
            doOnComplete();
        }
    }

    @Override
    public void cancel() {
        if (done.compareAndSet(false, true)) {
            super.cancel();
        }
    }

    protected void doOnNext(T t) {
        // NoOp by default
    }

    protected void doOnError(Throwable t) {
        // NoOp by default
    }

    protected void doOnComplete() {
        // NoOp by default
    }
}
