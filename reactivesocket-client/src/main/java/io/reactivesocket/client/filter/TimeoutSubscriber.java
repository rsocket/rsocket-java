/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket.client.filter;

import io.reactivesocket.client.exception.TimeoutException;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

class TimeoutSubscriber<T> implements Subscriber<T> {
    private final Subscriber<T> child;
    private final ScheduledExecutorService executor;
    private final long timeout;
    private final TimeUnit unit;
    private Subscription subscription;
    private ScheduledFuture<?> future;
    private boolean finished;

    TimeoutSubscriber(Subscriber<T> child, ScheduledExecutorService executor, long timeout, TimeUnit unit) {
        this.child = child;
        this.subscription = null;
        this.finished = false;
        this.timeout = timeout;
        this.unit = unit;
        this.executor = executor;
    }

    @Override
    public void onSubscribe(Subscription s) {
        subscription = s;
        future = executor.schedule(this::cancel, timeout, unit);
        child.onSubscribe(s);
    }

    @Override
    public synchronized void onNext(T t) {
        if (! finished) {
            child.onNext(t);
        }
    }

    @Override
    public synchronized void onError(Throwable t) {
        if (! finished) {
            finished = true;
            child.onError(t);
            if (future != null) {
                future.cancel(true);
            }
        }
    }

    @Override
    public synchronized void onComplete() {
        if (! finished) {
            finished = true;
            child.onComplete();
            if (future != null) {
                future.cancel(true);
            }
        }
    }

    private synchronized void cancel() {
        if (! finished) {
            subscription.cancel();
            child.onError(new TimeoutException());
            finished = true;
        }
    }
}
