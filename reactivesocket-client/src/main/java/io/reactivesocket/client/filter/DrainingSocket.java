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
import io.reactivesocket.rx.Completable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class DrainingSocket implements ReactiveSocket {
    private final ReactiveSocket child;
    private final AtomicInteger count;
    private volatile boolean closed;

    private class CountingSubscriber<T> implements Subscriber<T> {
        private final Subscriber<T> child;

        private CountingSubscriber(Subscriber<T> child) {
            this.child = child;
        }

        @Override
        public void onSubscribe(Subscription s) {
            child.onSubscribe(s);
            incr();
        }

        @Override
        public void onNext(T t) {
            child.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            child.onError(t);
            decr();
        }

        @Override
        public void onComplete() {
            child.onComplete();
            decr();
        }
    }

    public DrainingSocket(ReactiveSocket child) {
        this.child = child;
        count = new AtomicInteger(0);
        closed = false;
    }


    @Override
    public Publisher<Void> fireAndForget(Payload payload) {
        return subscriber ->
            child.fireAndForget(payload).subscribe(new CountingSubscriber<>(subscriber));
    }

    @Override
    public Publisher<Payload> requestResponse(Payload payload) {
        return subscriber ->
            child.requestResponse(payload).subscribe(new CountingSubscriber<>(subscriber));
    }

    @Override
    public Publisher<Payload> requestStream(Payload payload) {
        return subscriber ->
            child.requestStream(payload).subscribe(new CountingSubscriber<>(subscriber));
    }

    @Override
    public Publisher<Payload> requestSubscription(Payload payload) {
        return subscriber ->
            child.requestSubscription(payload).subscribe(new CountingSubscriber<>(subscriber));
    }

    @Override
    public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
        return subscriber ->
            child.requestChannel(payloads).subscribe(new CountingSubscriber<>(subscriber));
    }

    @Override
    public Publisher<Void> metadataPush(Payload payload) {
        return subscriber ->
            child.metadataPush(payload).subscribe(new CountingSubscriber<>(subscriber));
    }

    @Override
    public double availability() {
        if (closed) {
            return 0.0;
        } else {
            return child.availability();
        }
    }

    @Override
    public void start(Completable c) {
        child.start(c);
    }

    @Override
    public void onRequestReady(Consumer<Throwable> c) {
        child.onRequestReady(c);
    }

    @Override
    public void onRequestReady(Completable c) {
        child.onRequestReady(c);
    }

    @Override
    public void onShutdown(Completable c) {
        child.onShutdown(c);
    }

    @Override
    public void sendLease(int ttl, int numberOfRequests) {
        child.sendLease(ttl, numberOfRequests);
    }

    @Override
    public void shutdown() {
        closed = true;
        if (count.get() == 0) {
            child.shutdown();
        }
    }

    @Override
    public void close() throws Exception {
        closed = true;
        if (count.get() == 0) {
            child.close();
        }
    }

    private void incr() {
        count.incrementAndGet();
    }

    private void decr() {
        int n = count.decrementAndGet();
        if (closed && n == 0) {
            try {
                child.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public String toString() {
        return "DrainingSocket(closed=" + closed + ")->" + child;
    }
}
