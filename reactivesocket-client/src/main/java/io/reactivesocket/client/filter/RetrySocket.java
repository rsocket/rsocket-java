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

import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.util.ReactiveSocketProxy;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

public class RetrySocket extends ReactiveSocketProxy {
    private final int retry;
    private final Function<Throwable, Boolean> retryThisException;

    public RetrySocket(ReactiveSocket child, int retry, Function<Throwable, Boolean> retryThisException) {
        super(child);
        this.retry = retry;
        this.retryThisException = retryThisException;
    }

    @Override
    public Publisher<Void> fireAndForget(Payload payload) {
        return subscriber -> child.fireAndForget(payload).subscribe(
            new RetrySubscriber<>(subscriber, () -> child.fireAndForget(payload))
        );
    }

    @Override
    public Publisher<Payload> requestResponse(Payload payload) {
        return subscriber -> child.requestResponse(payload).subscribe(
            new RetrySubscriber<>(subscriber, () -> child.requestResponse(payload))
        );
    }

    @Override
    public Publisher<Payload> requestStream(Payload payload) {
        return subscriber -> child.requestStream(payload).subscribe(
            new RetrySubscriber<>(subscriber, () -> child.requestStream(payload))
        );
    }

    @Override
    public Publisher<Payload> requestSubscription(Payload payload) {
        return subscriber -> child.requestSubscription(payload).subscribe(
            new RetrySubscriber<>(subscriber, () -> child.requestSubscription(payload))
        );
    }

    @Override
    public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
        return subscriber -> child.requestChannel(payloads).subscribe(
            new RetrySubscriber<>(subscriber, () -> child.requestChannel(payloads))
        );
    }

    @Override
    public Publisher<Void> metadataPush(Payload payload) {
        return subscriber -> child.metadataPush(payload).subscribe(
            new RetrySubscriber<>(subscriber, () -> child.metadataPush(payload))
        );
    }

    private class RetrySubscriber<T> implements Subscriber<T> {
        private final Subscriber<? super T> child;
        private Supplier<Publisher<T>> action;
        private AtomicInteger budget;

        private RetrySubscriber(Subscriber<? super T> child, Supplier<Publisher<T>> action) {
            this.child = child;
            this.action = action;
            this.budget = new AtomicInteger(retry);
        }

        @Override
        public void onSubscribe(Subscription s) {
            child.onSubscribe(s);
        }

        @Override
        public void onNext(T t) {
            child.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            if (budget.decrementAndGet() > 0 && retryThisException.apply(t)) {
                action.get().subscribe(this);
            } else {
                child.onError(t);
            }
        }

        @Override
        public void onComplete() {
            child.onComplete();
        }
    }
}
