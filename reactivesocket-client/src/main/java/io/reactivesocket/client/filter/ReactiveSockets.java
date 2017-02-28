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

import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.util.ReactiveSocketProxy;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public final class ReactiveSockets {

    private ReactiveSockets() {
        // No Instances.
    }

    /**
     * Provides a mapping function to wrap a {@code ReactiveSocket} such that all requests will timeout, if not
     * completed after the specified {@code timeout}.
     *
     * @param timeout timeout duration.
     *
     * @return Function to transform any socket into a timeout socket.
     */
    public static Function<ReactiveSocket, ReactiveSocket> timeout(Duration timeout) {
        return source -> new ReactiveSocketProxy(source) {
            @Override
            public Mono<Void> fireAndForget(Payload payload) {
                return source.fireAndForget(payload).timeout(timeout);
            }

            @Override
            public Mono<Payload> requestResponse(Payload payload) {
                return source.requestResponse(payload).timeout(timeout);
            }

            @Override
            public Flux<Payload> requestStream(Payload payload) {
                return source.requestStream(payload).timeout(timeout);
            }

            @Override
            public Flux<Payload> requestSubscription(Payload payload) {
                return source.requestSubscription(payload).timeout(timeout);
            }

            @Override
            public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                return source.requestChannel(payloads).timeout(timeout);
            }

            @Override
            public Mono<Void> metadataPush(Payload payload) {
                return source.metadataPush(payload).timeout(timeout);
            }
        };
    }

    /**
     * Provides a mapping function to wrap a {@code ReactiveSocket} such that a call to {@link ReactiveSocket#close()}
     * does not cancel all pending requests. Instead, it will wait for all pending requests to finish and then close
     * the socket.
     *
     * @return Function to transform any socket into a safe closing socket.
     */
    public static Function<ReactiveSocket, ReactiveSocket> safeClose() {
        return source -> new ReactiveSocketProxy(source) {
            final AtomicInteger count = new AtomicInteger();
            final AtomicBoolean closed = new AtomicBoolean();

            @Override
            public Mono<Void> fireAndForget(Payload payload) {
                return source.fireAndForget(payload)
                    .doOnSubscribe(s -> count.incrementAndGet())
                    .doFinally(signalType -> {
                        if (count.decrementAndGet() == 0 && closed.get()) {
                            source.close().subscribe();
                        }
                    });
            }

            @Override
            public Mono<Payload> requestResponse(Payload payload) {
                return source.requestResponse(payload)
                    .doOnSubscribe(s -> count.incrementAndGet())
                    .doFinally(signalType -> {
                        if (count.decrementAndGet() == 0 && closed.get()) {
                            source.close().subscribe();
                        }
                    });
            }

            @Override
            public Flux<Payload> requestStream(Payload payload) {
                return source.requestStream(payload)
                    .doOnSubscribe(s -> count.incrementAndGet())
                    .doFinally(signalType -> {
                        if (count.decrementAndGet() == 0 && closed.get()) {
                            source.close().subscribe();
                        }
                    });
            }

            @Override
            public Flux<Payload> requestSubscription(Payload payload) {
                return source.requestSubscription(payload)
                    .doOnSubscribe(s -> count.incrementAndGet())
                    .doFinally(signalType -> {
                        if (count.decrementAndGet() == 0 && closed.get()) {
                            source.close().subscribe();
                        }
                    });
            }

            @Override
            public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                return source.requestChannel(payloads)
                    .doOnSubscribe(s -> count.incrementAndGet())
                    .doFinally(signalType -> {
                        if (count.decrementAndGet() == 0 && closed.get()) {
                            source.close().subscribe();
                        }
                    });
            }

            @Override
            public Mono<Void> metadataPush(Payload payload) {
                return source.metadataPush(payload)
                    .doOnSubscribe(s -> count.incrementAndGet())
                    .doFinally(signalType -> {
                        if (count.decrementAndGet() == 0 && closed.get()) {
                            source.close().subscribe();
                        }
                    });
            }

            @Override
            public Mono<Void> close() {
                return Mono.defer(() -> {
                    if (closed.compareAndSet(false, true)) {
                        if (count.get() == 0) {
                            return source.close();
                        } else {
                            return source.onClose();
                        }
                    }
                    return source.onClose();
                });
            }
        };
    }
}
