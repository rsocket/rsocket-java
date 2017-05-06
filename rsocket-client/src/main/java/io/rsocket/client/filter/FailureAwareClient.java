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
package io.rsocket.client.filter;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.client.RSocketClient;
import io.rsocket.stat.Ewma;
import io.rsocket.util.Clock;
import io.rsocket.util.RSocketProxy;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeUnit;

/**
 * This child compute the error rate of a particular remote location and adapt the availability
 * of the RSocketFactory but also of the RSocket.
 *
 * It means that if a remote host doesn't generate lots of errors when connecting to it, but a
 * lot of them when sending messages, we will still decrease the availability of the child
 * reducing the probability of connecting to it.
 */
public class FailureAwareClient implements RSocketClient {

    private static final double EPSILON = 1e-4;

    private final RSocketClient delegate;
    private final long tau;
    private long stamp;
    private final Ewma errorPercentage;

    public FailureAwareClient(RSocketClient delegate, long halfLife, TimeUnit unit) {
        this.delegate = delegate;
        this.tau = Clock.unit().convert((long)(halfLife / Math.log(2)), unit);
        this.stamp = Clock.now();
        errorPercentage = new Ewma(halfLife, unit, 1.0);
    }

    public FailureAwareClient(RSocketClient delegate) {
        this(delegate, 5, TimeUnit.SECONDS);
    }

    @Override
    public Mono<? extends RSocket> connect() {
        return delegate.connect()
            .doOnNext(o -> updateErrorPercentage(1.0))
            .doOnError(t ->  updateErrorPercentage(0.0))
            .map(socket -> new RSocketProxy(socket) {
                @Override
                public Mono<Void> fireAndForget(Payload payload) {
                    return source.fireAndForget(payload)
                            .doOnError(t -> errorPercentage.insert(0.0))
                            .doOnSuccess(v -> updateErrorPercentage(1.0));
                }

                @Override
                public Mono<Payload> requestResponse(Payload payload) {
                    return source.requestResponse(payload)
                            .doOnError(t -> errorPercentage.insert(0.0))
                            .doOnSuccess(p -> updateErrorPercentage(1.0));
                }

                @Override
                public Flux<Payload> requestStream(Payload payload) {
                    return source.requestStream(payload)
                            .doOnError(th -> errorPercentage.insert(0.0))
                            .doOnComplete(() -> updateErrorPercentage(1.0));
                }

                @Override
                public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                    return source.requestChannel(payloads)
                            .doOnError(th -> errorPercentage.insert(0.0))
                            .doOnComplete(() -> updateErrorPercentage(1.0));
                }

                @Override
                public Mono<Void> metadataPush(Payload payload) {
                    return source.metadataPush(payload)
                            .doOnError(t -> errorPercentage.insert(0.0))
                            .doOnSuccess(v -> updateErrorPercentage(1.0));
                }

                @Override
                public double availability() {
                    // If the window is expired set success and failure to zero and return
                    // the child availability
                    if (Clock.now() - stamp > tau) {
                        updateErrorPercentage(1.0);
                    }
                    return source.availability() * errorPercentage.value();
                }
            });
    }

    @Override
    public double availability() {
        double e = errorPercentage.value();
        if (Clock.now() - stamp > tau) {
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

    private synchronized void updateErrorPercentage(double value) {
        errorPercentage.insert(value);
        stamp = Clock.now();
    }

    @Override
    public String toString() {
        return "FailureAwareClient(" + errorPercentage.value() + ")->" + delegate;
    }
}