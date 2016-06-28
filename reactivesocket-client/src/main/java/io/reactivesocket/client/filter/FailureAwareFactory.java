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
import io.reactivesocket.ReactiveSocketFactory;
import io.reactivesocket.exceptions.TransportException;
import io.reactivesocket.client.util.Clock;
import io.reactivesocket.client.stat.Ewma;
import io.reactivesocket.util.ReactiveSocketProxy;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * This child compute the error rate of a particular remote location and adapt the availability
 * of the ReactiveSocketFactory but also of the ReactiveSocket.
 *
 * It means that if a remote host doesn't generate lots of errors when connecting to it, but a
 * lot of them when sending messages, we will still decrease the availability of the child
 * reducing the probability of connecting to it.
 *
 * @param <T> the identifier for the remote server (most likely SocketAddress)
 */
public class FailureAwareFactory<T> implements ReactiveSocketFactory<T> {
    private static final double EPSILON = 1e-4;

    private final ReactiveSocketFactory<T> child;
    private final long tau;
    private long stamp;
    private Ewma errorPercentage;

    public FailureAwareFactory(ReactiveSocketFactory<T> child, long halfLife, TimeUnit unit) {
        this.child = child;
        this.tau = Clock.unit().convert((long)(halfLife / Math.log(2)), unit);
        this.stamp = Clock.now();
        errorPercentage = new Ewma(halfLife, unit, 1.0);
    }

    public FailureAwareFactory(ReactiveSocketFactory<T> child) {
        this(child, 5, TimeUnit.SECONDS);
    }

    @Override
    public Publisher<ReactiveSocket> apply() {
        return subscriber -> child.apply().subscribe(new Subscriber<ReactiveSocket>() {
            @Override
            public void onSubscribe(Subscription s) {
                subscriber.onSubscribe(s);
            }

            @Override
            public void onNext(ReactiveSocket reactiveSocket) {
                updateErrorPercentage(1.0);
                ReactiveSocket wrapped = new FailureAwareReactiveSocket(reactiveSocket);
                subscriber.onNext(wrapped);
            }

            @Override
            public void onError(Throwable t) {
                updateErrorPercentage(0.0);
                subscriber.onError(t);
            }

            @Override
            public void onComplete() {
                subscriber.onComplete();
            }
        });
    }

    public double availability() {
        double e = errorPercentage.value();
        if ((Clock.now() - stamp) > tau) {
            // If the window is expired artificially increase the availability
            double a = Math.min(1.0, e + 0.5);
            errorPercentage.reset(a);
        }
        if (e < EPSILON) {
            e = 0.0;
        } else if (1.0 - EPSILON < e) {
            e = 1.0;
        }

        return e;
    }

    @Override
    public T remote() {
        return child.remote();
    }

    private synchronized void updateErrorPercentage(double value) {
        errorPercentage.insert(value);
        stamp = Clock.now();
    }

    @Override
    public String toString() {
        return "FailureAwareFactory(" + errorPercentage.value() + ") ~> " + child.toString();
    }

    /**
     * ReactiveSocket wrapper that update the statistics associated with a remote server
     */
    private class FailureAwareReactiveSocket extends ReactiveSocketProxy {
        private class InnerSubscriber<U> implements Subscriber<U> {
            private final Subscriber<U> child;

            InnerSubscriber(Subscriber<U> child) {
                this.child = child;
            }

            @Override
            public void onSubscribe(Subscription s) {
                child.onSubscribe(s);
            }

            @Override
            public void onNext(U u) {
                child.onNext(u);
            }

            @Override
            public void onError(Throwable t) {
                errorPercentage.insert(0.0);
                child.onError(t);
            }

            @Override
            public void onComplete() {
                updateErrorPercentage(1.0);
                child.onComplete();
            }
        }

        FailureAwareReactiveSocket(ReactiveSocket child) {
            super(child);
        }

        @Override
        public double availability() {
            double childAvailability = child.availability();
            // If the window is expired set success and failure to zero and return
            // the child availability
            if ((Clock.now() - stamp) > tau) {
                updateErrorPercentage(1.0);
            }
            return childAvailability * errorPercentage.value();
        }

        @Override
        public Publisher<Payload> requestResponse(Payload payload) {
            return subscriber ->
                child.requestResponse(payload).subscribe(new InnerSubscriber<>(subscriber));
        }

        @Override
        public Publisher<Payload> requestSubscription(Payload payload) {
            return subscriber ->
                child.requestSubscription(payload).subscribe(new InnerSubscriber<>(subscriber));
        }

        @Override
        public Publisher<Payload> requestStream(Payload payload) {
            return subscriber ->
                child.requestStream(payload).subscribe(new InnerSubscriber<>(subscriber));
        }

        @Override
        public Publisher<Void> fireAndForget(Payload payload) {
            return subscriber ->
                child.fireAndForget(payload).subscribe(new InnerSubscriber<>(subscriber));
        }

        @Override
        public Publisher<Void> metadataPush(Payload payload) {
            return child.metadataPush(payload);
        }

        @Override
        public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
            return subscriber ->
                child.requestChannel(payloads).subscribe(new InnerSubscriber<>(subscriber));
        }

        @Override
        public String toString() {
            return "FailureAwareReactiveSocket(" + errorPercentage.value() + ") ~> " + child.toString();
        }
    }

    public static <W>
    Function<ReactiveSocketFactory<W>, ReactiveSocketFactory<W>> filter(long tau, TimeUnit unit) {
        return f -> new FailureAwareFactory<>(f, tau, unit);
    }
}
