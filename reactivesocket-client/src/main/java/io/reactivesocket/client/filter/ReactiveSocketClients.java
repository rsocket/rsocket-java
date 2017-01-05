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
import io.reactivesocket.reactivestreams.extensions.Scheduler;
import org.reactivestreams.Publisher;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * A collection of various different clients that are available.
 */
public final class ReactiveSocketClients {

    private ReactiveSocketClients() {
        // No Instances.
    }

    /**
     * Wraps a {@code ReactiveSocketClient} such that all {@link ReactiveSocketClient#connect()} calls will timeout,
     * if not completed after the specified {@code timeout}.
     *
     * @param orig Client to wrap.
     * @param timeout timeout duration.
     * @param unit timeout duration unit.
     * @param scheduler scheduler for timeout.
     *
     * @return New client that imposes the passed {@code timeout}.
     */
    public static ReactiveSocketClient connectTimeout(ReactiveSocketClient orig, long timeout, TimeUnit unit,
                                                      Scheduler scheduler) {
        return new AbstractReactiveSocketClient(orig) {
            @Override
            public Publisher<? extends ReactiveSocket> connect() {
                return Px.from(orig.connect()).timeout(timeout, unit, scheduler);
            }

            @Override
            public double availability() {
                return orig.availability();
            }
        };
    }

    /**
     * Wraps a {@code ReactiveSocketClient} such that it's availability as returned by
     * {@link ReactiveSocketClient#availability()} is adjusted according to the errors received from the client for all
     * requests.
     *
     * @return New client that changes availability based on failures.
     *
     * @see FailureAwareClient
     */
    public static ReactiveSocketClient detectFailures(ReactiveSocketClient orig) {
        return new FailureAwareClient(orig);
    }

    /**
     * Wraps the provided client with a mapping function to modify each {@link ReactiveSocket} created by the client.
     *
     * @param orig Original client to wrap.
     * @param mapper Mapping function to modify every {@code ReactiveSocket} created by the original client.
     *
     * @return A new client wrapping the original.
     */
    public static ReactiveSocketClient wrap(ReactiveSocketClient orig, Function<ReactiveSocket, ReactiveSocket> mapper) {
        return new AbstractReactiveSocketClient(orig) {
            @Override
            public Publisher<? extends ReactiveSocket> connect() {
                return Px.from(orig.connect()).map(mapper::apply);
            }

            @Override
            public double availability() {
                return orig.availability();
            }
        };
    }
}
