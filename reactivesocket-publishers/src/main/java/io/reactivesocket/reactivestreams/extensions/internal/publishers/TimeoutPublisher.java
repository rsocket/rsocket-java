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
import io.reactivesocket.reactivestreams.extensions.Scheduler;
import io.reactivesocket.reactivestreams.extensions.internal.subscribers.CancellableSubscriber;
import io.reactivesocket.reactivestreams.extensions.internal.subscribers.Subscribers;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class TimeoutPublisher<T> implements Px<T> {

    @SuppressWarnings("ThrowableInstanceNeverThrown")
    private static final TimeoutException timeoutException = new TimeoutException() {
        private static final long serialVersionUID = 6195545973881750858L;

        @Override
        public synchronized Throwable fillInStackTrace() {
            return this;
        }
    };

    private final Publisher<T> child;
    private final Scheduler scheduler;
    private final long timeout;
    private final TimeUnit unit;

    public TimeoutPublisher(Publisher<T> child, long timeout, TimeUnit unit, Scheduler scheduler) {
        this.child = child;
        this.timeout = timeout;
        this.unit = unit;
        this.scheduler = scheduler;
    }

    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
        Runnable onTimeout = () -> subscriber.onError(timeoutException);
        CancellableSubscriber<Void> timeoutSub = Subscribers.create(null, null, throwable -> onTimeout.run(),
                                                                    onTimeout, null);
        Runnable cancelTimeout = () -> timeoutSub.cancel();
        Subscriber<T> sourceSub = Subscribers.create(subscription -> subscriber.onSubscribe(subscription),
                                                     t -> {
                                                         cancelTimeout.run();
                                                         subscriber.onNext(t);
                                                     },
                                                     throwable -> {
                                                         cancelTimeout.run();
                                                         subscriber.onError(throwable);
                                                     },
                                                     () -> {
                                                         cancelTimeout.run();
                                                         subscriber.onComplete();
                                                     },
                                                     cancelTimeout);
        child.subscribe(sourceSub);
        scheduler.timer(timeout, unit).subscribe(timeoutSub);
    }
}
