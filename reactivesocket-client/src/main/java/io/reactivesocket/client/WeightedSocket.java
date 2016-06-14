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
package io.reactivesocket.client;

import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.client.util.Clock;
import io.reactivesocket.client.stat.Ewma;
import io.reactivesocket.client.stat.Median;
import io.reactivesocket.client.stat.Quantile;
import io.reactivesocket.util.ReactiveSocketProxy;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Wrapper of a ReactiveSocket, it computes statistics about the req/resp calls and
 * update availability accordingly.
 */
public class WeightedSocket extends ReactiveSocketProxy {
    private static final double STARTUP_PENALTY = Long.MAX_VALUE >> 12;

    private final ReactiveSocket child;
    private final Quantile lowerQuantile;
    private final Quantile higherQuantile;
    private final long inactivityFactor;

    private volatile int pending;       // instantaneous rate
    private long stamp;                 // last timestamp we sent a request
    private long stamp0;                // last timestamp we sent a request or receive a response
    private long duration;              // instantaneous cumulative duration

    private Median median;
    private Ewma interArrivalTime;

    private AtomicLong pendingStreams;  // number of active streams

    public WeightedSocket(ReactiveSocket child, Quantile lowerQuantile, Quantile higherQuantile, int inactivityFactor) {
        super(child);
        this.child = child;
        this.lowerQuantile = lowerQuantile;
        this.higherQuantile = higherQuantile;
        this.inactivityFactor = inactivityFactor;
        long now = Clock.now();
        this.stamp = now;
        this.stamp0 = now;
        this.duration = 0L;
        this.pending = 0;
        this.median = new Median();
        this.interArrivalTime = new Ewma(1, TimeUnit.MINUTES, 1000);
        this.pendingStreams = new AtomicLong();
    }

    public WeightedSocket(ReactiveSocket child, Quantile lowerQuantile, Quantile higherQuantile) {
        this(child, lowerQuantile, higherQuantile, 100);
    }

    @Override
    public Publisher<Payload> requestResponse(Payload payload) {
        return subscriber ->
            child.requestResponse(payload).subscribe(new LatencySubscriber<>(subscriber));
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
    public Publisher<Void> fireAndForget(Payload payload) {
        return subscriber ->
            child.fireAndForget(payload).subscribe(new CountingSubscriber<>(subscriber));
    }

    @Override
    public Publisher<Void> metadataPush(Payload payload) {
        return subscriber ->
            child.metadataPush(payload).subscribe(new CountingSubscriber<>(subscriber));
    }

    @Override
    public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
        return subscriber ->
            child.requestChannel(payloads).subscribe(new CountingSubscriber<>(subscriber));
    }

    public synchronized double getPredictedLatency() {
        long now = Clock.now();
        long elapsed = Math.max(now - stamp, 1L);

        double weight;
        double prediction = median.estimation();

        if (prediction == 0.0) {
            if (pending == 0) {
                weight = 0.0; // first request
            } else {
                // subsequent requests while we don't have any history
                weight = STARTUP_PENALTY + pending;
            }
        } else if (pending == 0 && elapsed > inactivityFactor * interArrivalTime.value()) {
            // if we did't see any data for a while, we decay the prediction by inserting
            // artificial 0.0 into the median
            median.insert(0.0);
            weight = median.estimation();
        } else {
            double predicted = prediction * pending;
            double instant = instantaneous(now);

            if (predicted < instant) { // NB: (0.0 < 0.0) == false
                weight = instant / pending; // NB: pending never equal 0 here
            } else {
                // we are under the predictions
                weight = prediction;
            }
        }

        return weight;
    }

    public int getPending() {
        return pending;
    }

    private synchronized long instantaneous(long now) {
        return duration + (now - stamp0) * pending;
    }

    private synchronized long incr() {
        long now = Clock.now();
        interArrivalTime.insert(now - stamp);
        duration += Math.max(0, now - stamp0) * pending;
        pending += 1;
        stamp = now;
        stamp0 = now;
        return now;
    }

    private synchronized long decr(long timestamp) {
        long now = Clock.now();
        duration += Math.max(0, now - stamp0) * pending - (now - timestamp);
        pending -= 1;
        stamp0 = now;
        return now;
    }

    private synchronized void observe(double rtt) {
        median.insert(rtt);
        lowerQuantile.insert(rtt);
        higherQuantile.insert(rtt);
    }

    @Override
    public void close() throws Exception {
        child.close();
    }

    @Override
    public String toString() {
        return "WeightedSocket@" + hashCode()
            + " [median:" + median.estimation()
            + " quantile-low:" + lowerQuantile.estimation()
            + " quantile-high:" + higherQuantile.estimation()
            + " inter-arrival:" + interArrivalTime.value()
            + " duration/pending:" + (pending == 0 ? 0 : (double)duration / pending)
            + " availability: " + availability()
            + "]->" + child.toString();
    }

    /**
     * Subscriber wrapper used for request/response interaction model, measure and collect
     * latency information.
     */
    private class LatencySubscriber<T> implements Subscriber<T> {
        private final Subscriber<T> child;
        private long start;

        LatencySubscriber(Subscriber<T> child) {
            this.child = child;
        }

        @Override
        public void onSubscribe(Subscription s) {
            child.onSubscribe(s);
            start = incr();
        }

        @Override
        public void onNext(T t) {
            child.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            child.onError(t);
            decr(start);
        }

        @Override
        public void onComplete() {
            long now = decr(start);
            observe(now - start);
            child.onComplete();
        }
    }

    /**
     * Subscriber wrapper used for stream like interaction model, it only counts the number of
     * active streams
     */
    private class CountingSubscriber<T> implements Subscriber<T> {
        private final Subscriber<T> child;

        CountingSubscriber(Subscriber<T> child) {
            this.child = child;
        }

        @Override
        public void onSubscribe(Subscription s) {
            pendingStreams.incrementAndGet();
            child.onSubscribe(s);
        }

        @Override
        public void onNext(T t) {
            child.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            pendingStreams.decrementAndGet();
            child.onError(t);
        }

        @Override
        public void onComplete() {
            pendingStreams.decrementAndGet();
            child.onComplete();
        }
    }
}
