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
package io.reactivesocket.client.filter;

import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.client.AbstractReactiveSocketClient;
import io.reactivesocket.client.ReactiveSocketClient;
import io.reactivesocket.reactivestreams.extensions.Px;
import io.reactivesocket.stat.Ewma;
import io.reactivesocket.util.Clock;
import io.reactivesocket.util.ReactiveSocketDecorator;
import org.reactivestreams.Publisher;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * This child compute the error rate of a particular remote location and adapt the availability
 * of the ReactiveSocketFactory but also of the ReactiveSocket.
 *
 * It means that if a remote host doesn't generate lots of errors when connecting to it, but a
 * lot of them when sending messages, we will still decrease the availability of the child
 * reducing the probability of connecting to it.
 */
public class FailureAwareClient extends AbstractReactiveSocketClient {

    private static final double EPSILON = 1e-4;

    private final ReactiveSocketClient delegate;
    private final long tau;
    private long stamp;
    private final Ewma errorPercentage;

    public FailureAwareClient(ReactiveSocketClient delegate, long halfLife, TimeUnit unit) {
        super(delegate);
        this.delegate = delegate;
        this.tau = Clock.unit().convert((long)(halfLife / Math.log(2)), unit);
        this.stamp = Clock.now();
        errorPercentage = new Ewma(halfLife, unit, 1.0);
    }

    public FailureAwareClient(ReactiveSocketClient delegate) {
        this(delegate, 5, TimeUnit.SECONDS);
    }

    @Override
    public Publisher<? extends ReactiveSocket> connect() {
        return Px.from(delegate.connect())
                 .doOnNext(o -> updateErrorPercentage(1.0))
                 .doOnError(t ->  updateErrorPercentage(0.0))
                 .map(socket ->
                     ReactiveSocketDecorator.wrap(socket)
                                            .availability(delegate -> {
                                                // If the window is expired set success and failure to zero and return
                                                // the child availability
                                                if (Clock.now() - stamp > tau) {
                                                    updateErrorPercentage(1.0);
                                                }
                                                return delegate.availability() * errorPercentage.value();
                                            })
                                            .decorateAllResponses(_record())
                                            .decorateAllVoidResponses(_record())
                                            .finish()
                 );
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

    private <T> Function<Publisher<T>, Publisher<T>> _record() {
        return t -> Px.from(t)
                      .doOnError(th -> errorPercentage.insert(0.0))
                      .doOnComplete(() -> updateErrorPercentage(1.0));
    }

    @Override
    public String toString() {
        return "FailureAwareClient(" + errorPercentage.value() + ")->" + delegate;
    }
}
