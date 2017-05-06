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

import io.rsocket.RSocket;
import io.rsocket.client.RSocketClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.function.Function;

/**
 * A collection of various different clients that are available.
 */
public final class RSocketClients {

    private RSocketClients() {
        // No Instances.
    }

    /**
     * Wraps a {@code RSocketClient} such that all {@link RSocketClient#connect()} calls will timeout,
     * if not completed after the specified {@code timeout}.
     *
     * @param orig Client to wrap.
     * @param timeout timeout duration.
     *
     * @return New client that imposes the passed {@code timeout}.
     */
    public static RSocketClient connectTimeout(RSocketClient orig, Duration timeout) {
        return new RSocketClient() {
            @Override
            public Mono<? extends RSocket> connect() {
                return orig.connect().timeout(timeout);
            }

            @Override
            public double availability() {
                return orig.availability();
            }
        };
    }

    /**
     * Wraps a {@code RSocketClient} such that it's availability as returned by
     * {@link RSocketClient#availability()} is adjusted according to the errors received from the client for all
     * requests.
     *
     * @return New client that changes availability based on failures.
     *
     * @see FailureAwareClient
     */
    public static RSocketClient detectFailures(RSocketClient orig) {
        return new FailureAwareClient(orig);
    }

    /**
     * Wraps the provided client with a mapping function to modify each {@link RSocket} created by the client.
     *
     * @param orig Original client to wrap.
     * @param mapper Mapping function to modify every {@code RSocket} created by the original client.
     *
     * @return A new client wrapping the original.
     */
    public static RSocketClient wrap(RSocketClient orig, Function<RSocket, RSocket> mapper) {
        return new RSocketClient() {
            @Override
            public Mono<? extends RSocket> connect() {
                return orig.connect().map(mapper);
            }

            @Override
            public double availability() {
                return orig.availability();
            }
        };
    }
}
