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

class SingleEmissionSubscription<T> implements Subscription {

    private final Subscriber<? super T> subscriber;
    private final Throwable error;
    private final T item;
    private boolean done;

    public SingleEmissionSubscription(Subscriber<? super T> subscriber, Throwable error) {
        this.subscriber = subscriber;
        this.error = error;
        item = null;
    }

    public SingleEmissionSubscription(Subscriber<? super T> subscriber, T item) {
        this.subscriber = subscriber;
        error = null;
        this.item = item;
    }

    public SingleEmissionSubscription(Subscriber<? super T> subscriber) {
        this.subscriber = subscriber;
        error = null;
        item = null;
    }

    @Override
    public void request(long n) {
        boolean _emit = false;
        synchronized (this) {
            if (!done) {
                done = true;
                _emit = true;
            }
        }

        if (_emit) {
            if (error != null) {
                subscriber.onError(error);
            } else if (item != null) {
                subscriber.onNext(item);
                subscriber.onComplete();
            } else {
                subscriber.onComplete();
            }
        }
    }

    @Override
    public void cancel() {
        // No Op since this is the starting publisher
    }
}
