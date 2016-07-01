/**
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
package io.reactivesocket.util;

import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.rx.Completable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.function.Consumer;
import java.util.function.Function;


/**
 * Wrapper/Proxy for a ReactiveSocket.
 * This is useful when we want to override a specific method.
 */
public class ReactiveSocketProxy implements ReactiveSocket {
    protected final ReactiveSocket child;
    private final Function<Subscriber<? super Payload>, Subscriber<? super Payload>> subscriberWrapper;

    public ReactiveSocketProxy(ReactiveSocket child, Function<Subscriber<? super Payload>, Subscriber<? super Payload>> subscriberWrapper) {
        this.child = child;
        this.subscriberWrapper = subscriberWrapper;
    }

    public ReactiveSocketProxy(ReactiveSocket child) {
        this(child, null);
    }

    @Override
    public Publisher<Void> fireAndForget(Payload payload) {
        return child.fireAndForget(payload);
    }

    @Override
    public Publisher<Payload> requestResponse(Payload payload) {
        if (subscriberWrapper == null) {
            return child.requestResponse(payload);
        } else {
            return s -> {
                Subscriber<? super Payload> subscriber = subscriberWrapper.apply(s);
                child.requestResponse(payload).subscribe(subscriber);
            };
        }
    }

    @Override
    public Publisher<Payload> requestStream(Payload payload) {
        if (subscriberWrapper == null) {
            return child.requestStream(payload);
        } else {
            return s -> {
                Subscriber<? super Payload> subscriber = subscriberWrapper.apply(s);
                child.requestStream(payload).subscribe(subscriber);
            };
        }
    }

    @Override
    public Publisher<Payload> requestSubscription(Payload payload) {
        if (subscriberWrapper == null) {
            return child.requestSubscription(payload);
        } else {
            return s -> {
                Subscriber<? super Payload> subscriber = subscriberWrapper.apply(s);
                child.requestSubscription(payload).subscribe(subscriber);
            };
        }
    }

    @Override
    public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
        if (subscriberWrapper == null) {
            return child.requestChannel(payloads);
        } else {
            return s -> {
                Subscriber<? super Payload> subscriber = subscriberWrapper.apply(s);
                child.requestChannel(payloads).subscribe(subscriber);
            };
        }
    }

    @Override
    public Publisher<Void> metadataPush(Payload payload) {
        return child.metadataPush(payload);
    }

    @Override
    public double availability() {
        return child.availability();
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
        child.shutdown();
    }

    @Override
    public void close() throws Exception {
        child.close();
    }

    @Override
    public String toString() {
        return "ReactiveSocketProxy(" + child + ")";
    }
}