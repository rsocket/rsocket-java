/*
 * Copyright 2017 Netflix, Inc.
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
package io.reactivesocket.spectator;

import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.spectator.internal.HdrHistogramPercentileTimer;
import io.reactivesocket.spectator.internal.ThreadLocalAdderCounter;
import io.reactivesocket.util.ReactiveSocketProxy;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * An implementation of {@link ReactiveSocket} that sends metrics to Servo
 */
public class InstrumentedReactiveSocket extends ReactiveSocketProxy {
    final ThreadLocalAdderCounter success;
    final ThreadLocalAdderCounter failure;
    final HdrHistogramPercentileTimer timer;

    private class RecordingSubscriber<T> implements Subscriber<T> {
        private final Subscriber<T> child;
        private long start;

        RecordingSubscriber(Subscriber<T> child) {
            this.child = child;
        }

        @Override
        public void onSubscribe(Subscription s) {
            child.onSubscribe(s);
            start = recordStart();
        }

        @Override
        public void onNext(T t) {
            child.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            child.onError(t);
            recordFailure(start);
        }

        @Override
        public void onComplete() {
            child.onComplete();
            recordSuccess(start);
        }
    }

    public InstrumentedReactiveSocket(ReactiveSocket child, String prefix) {
        super(child);
        success = new ThreadLocalAdderCounter("success", prefix);
        failure = new ThreadLocalAdderCounter("failure", prefix);
        timer = new HdrHistogramPercentileTimer("latency", prefix);
    }

    @Override
    public Publisher<Payload> requestResponse(Payload payload) {
        return subscriber ->
            child.requestResponse(payload).subscribe(new RecordingSubscriber<>(subscriber));
    }

    @Override
    public Publisher<Payload> requestStream(Payload payload) {
        return subscriber ->
            child.requestStream(payload).subscribe(new RecordingSubscriber<>(subscriber));
    }

    @Override
    public Publisher<Payload> requestSubscription(Payload payload) {
        return subscriber ->
            child.requestSubscription(payload).subscribe(new RecordingSubscriber<>(subscriber));

    }

    @Override
    public Publisher<Void> fireAndForget(Payload payload) {
        return subscriber ->
            child.fireAndForget(payload).subscribe(new RecordingSubscriber<>(subscriber));

    }

    @Override
    public Publisher<Void> metadataPush(Payload payload) {
        return subscriber ->
            child.metadataPush(payload).subscribe(new RecordingSubscriber<>(subscriber));
    }

    @Override
    public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
        return subscriber ->
            child.requestChannel(payloads).subscribe(new RecordingSubscriber<>(subscriber));
    }

    public String histrogramToString() {
        long successCount = success.get();
        long failureCount = failure.get();
        long totalCount = successCount + failureCount;

        StringBuilder s = new StringBuilder();
        s.append(String.format("%-12s%-12s\n","Percentile","Latency"));
        s.append("=========================\n");
        s.append(String.format("%-12s%dms\n","50%",NANOSECONDS.toMillis(timer.getP50())));
        s.append(String.format("%-12s%dms\n","90%",NANOSECONDS.toMillis(timer.getP90())));
        s.append(String.format("%-12s%dms\n","99%",NANOSECONDS.toMillis(timer.getP99())));
        s.append(String.format("%-12s%dms\n","99.9%",NANOSECONDS.toMillis(timer.getP99_9())));
        s.append(String.format("%-12s%dms\n","99.99%",NANOSECONDS.toMillis(timer.getP99_99())));
        s.append("-------------------------\n");
        s.append(String.format("%-12s%dms\n","min",NANOSECONDS.toMillis(timer.getMin())));
        s.append(String.format("%-12s%dms\n","max",NANOSECONDS.toMillis(timer.getMax())));
        s.append(String.format("%-12s%d (%.0f%%)\n","success",successCount,100.0*successCount/totalCount));
        s.append(String.format("%-12s%d (%.0f%%)\n","failure",failureCount,100.0*failureCount/totalCount));
        s.append(String.format("%-12s%d\n","count",totalCount));
        return s.toString();
    }

    private static long recordStart() {
        return System.nanoTime();
    }

    private void recordFailure(long start) {
        failure.increment();
        timer.record(System.nanoTime() - start);
    }

    private void recordSuccess(long start) {
        success.increment();
        timer.record(System.nanoTime() - start);
    }
}
