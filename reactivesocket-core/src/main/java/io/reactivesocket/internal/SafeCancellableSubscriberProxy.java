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

abstract class SafeCancellableSubscriberProxy<T> extends SafeCancellableSubscriber<T> {

    private final Subscriber<? super T> delegate;

    protected SafeCancellableSubscriberProxy(Subscriber<? super T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void onSubscribe(final Subscription s) {
        Subscription s1 = new Subscription() {
            @Override
            public void request(long n) {
                s.request(n);
            }

            @Override
            public void cancel() {
                SafeCancellableSubscriberProxy.this.cancel();
            }
        };
        super.onSubscribe(s1);
        delegate.onSubscribe(s1);
    }

    @Override
    protected void doOnNext(T t) {
        delegate.onNext(t);
    }

    @Override
    protected void doOnError(Throwable t) {
        delegate.onError(t);
    }

    @Override
    protected void doOnComplete() {
        delegate.onComplete();
    }
}
