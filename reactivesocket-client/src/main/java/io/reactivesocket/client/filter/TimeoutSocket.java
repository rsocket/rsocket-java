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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TimeoutSocket extends ReactiveSocketProxy {
    private final ScheduledExecutorService executor;
    private final ReactiveSocket child;
    private final long timeout;
    private final TimeUnit unit;

    public TimeoutSocket(ReactiveSocket child, long timeout, TimeUnit unit, ScheduledExecutorService executor) {
        super(child);
        this.child = child;
        this.timeout = timeout;
        this.unit = unit;
        this.executor = executor;
    }

    public TimeoutSocket(ReactiveSocket child, long timeout, TimeUnit unit) {
        this(child, timeout, unit, Executors.newScheduledThreadPool(2));
    }

    @Override
    public Publisher<Void> fireAndForget(Payload payload) {
        return child.fireAndForget(payload);
    }

    @Override
    public Publisher<Payload> requestResponse(Payload payload) {
        return subscriber ->
            child.requestResponse(payload).subscribe(wrap(subscriber));
    }

    @Override
    public Publisher<Payload> requestStream(Payload payload) {
        return subscriber ->
            child.requestStream(payload).subscribe(wrap(subscriber));
    }

    @Override
    public Publisher<Payload> requestSubscription(Payload payload) {
        return subscriber ->
            child.requestSubscription(payload).subscribe(wrap(subscriber));
    }

    @Override
    public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
        return subscriber ->
            child.requestChannel(payloads).subscribe(wrap(subscriber));
    }

    private <T> Subscriber<T> wrap(Subscriber<T> subscriber) {
        return new TimeoutSubscriber<>(subscriber, executor, timeout, unit);
    }
}
