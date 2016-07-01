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

import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.ReactiveSocketConnector;
import io.reactivesocket.ReactiveSocketFactory;
import io.reactivesocket.client.filter.*;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ClientBuilder<T> {
    private static AtomicInteger counter = new AtomicInteger(0);
    private final String name;

    private final ScheduledExecutorService executor;

    private final long requestTimeout;
    private final TimeUnit requestTimeoutUnit;

    private final long connectTimeout;
    private final TimeUnit connectTimeoutUnit;

    private final double backupQuantile;

    private final int retries;

    private final ReactiveSocketConnector<T> connector;
    private final Function<Throwable, Boolean> retryThisException;

    private final Publisher<Collection<T>> source;

    private ClientBuilder(
        String name,
        ScheduledExecutorService executor,
        long requestTimeout, TimeUnit requestTimeoutUnit,
        long connectTimeout, TimeUnit connectTimeoutUnit,
        double backupQuantile,
        int retries, Function<Throwable, Boolean> retryThisException,
        ReactiveSocketConnector<T> connector,
        Publisher<Collection<T>> source
    ) {
        this.name = name;
        this.executor = executor;
        this.requestTimeout = requestTimeout;
        this.requestTimeoutUnit = requestTimeoutUnit;
        this.connectTimeout = connectTimeout;
        this.connectTimeoutUnit = connectTimeoutUnit;
        this.backupQuantile = backupQuantile;
        this.retries = retries;
        this.connector = connector;
        this.retryThisException = retryThisException;
        this.source = source;
    }

    public ClientBuilder<T> withRequestTimeout(long timeout, TimeUnit unit) {
        return new ClientBuilder<>(
            name,
            executor,
            timeout, unit,
            connectTimeout, connectTimeoutUnit,
            backupQuantile,
            retries, retryThisException,
            connector,
            source
        );
    }

    public ClientBuilder<T> withConnectTimeout(long timeout, TimeUnit unit) {
        return new ClientBuilder<>(
            name,
            executor,
            requestTimeout, requestTimeoutUnit,
            timeout, unit,
            backupQuantile,
            retries, retryThisException,
            connector,
            source
        );
    }

    public ClientBuilder<T> withExecutor(ScheduledExecutorService executor) {
        return new ClientBuilder<>(
            name,
            executor,
            requestTimeout, requestTimeoutUnit,
            connectTimeout, connectTimeoutUnit,
            backupQuantile,
            retries, retryThisException,
            connector,
            source
        );
    }

    public ClientBuilder<T> withConnector(ReactiveSocketConnector<T> connector) {
        return new ClientBuilder<>(
            name,
            executor,
            requestTimeout, requestTimeoutUnit,
            connectTimeout, connectTimeoutUnit,
            backupQuantile,
            retries, retryThisException,
            connector,
            source
        );
    }

    public ClientBuilder<T> withSource(Publisher<Collection<T>> source) {
        return new ClientBuilder<>(
            name,
            executor,
            requestTimeout, requestTimeoutUnit,
            connectTimeout, connectTimeoutUnit,
            backupQuantile,
            retries, retryThisException,
            connector,
            source
        );
    }

    public ReactiveSocket build() {
        if (source == null) {
            throw new IllegalStateException("Please configure the source!");
        }
        if (connector == null) {
            throw new IllegalStateException("Please configure the connector!");
        }

        ReactiveSocketConnector<T> filterConnector = connector
            .chain(socket -> new TimeoutSocket(socket, requestTimeout, requestTimeoutUnit, executor))
            .chain(DrainingSocket::new);

        Publisher<? extends Collection<ReactiveSocketFactory<T>>> factories =
            sourceToFactory(source, filterConnector);

        return new LoadBalancer<T>(factories);
    }

    private Publisher<? extends Collection<ReactiveSocketFactory<T>>> sourceToFactory(
        Publisher<? extends Collection<T>> source,
        ReactiveSocketConnector<T> connector
    ) {
        return subscriber ->
            source.subscribe(new Subscriber<Collection<T>>() {
                private Map<T, ReactiveSocketFactory<T>> current;

                @Override
                public void onSubscribe(Subscription s) {
                    subscriber.onSubscribe(s);
                    current = Collections.emptyMap();
                }

                @Override
                public void onNext(Collection<T> socketAddresses) {
                    Map<T, ReactiveSocketFactory<T>> next = new HashMap<>(socketAddresses.size());
                    for (T sa: socketAddresses) {
                        ReactiveSocketFactory<T> factory = current.get(sa);
                        if (factory == null) {
                            ReactiveSocketFactory<T> newFactory = connector.toFactory(sa);
                            newFactory = new TimeoutFactory<>(newFactory, connectTimeout, connectTimeoutUnit, executor);
                            newFactory = new FailureAwareFactory<>(newFactory);
                            next.put(sa, newFactory);
                        } else {
                            next.put(sa, factory);
                        }
                    }

                    current = next;
                    List<ReactiveSocketFactory<T>> factories = new ArrayList<>(current.values());
                    subscriber.onNext(factories);
                }

                @Override
                public void onError(Throwable t) { subscriber.onError(t); }

                @Override
                public void onComplete() { subscriber.onComplete(); }
            });
    }

    public static <T> ClientBuilder<T> instance() {
        return new ClientBuilder<>(
            "rs-loadbalancer-" + counter.incrementAndGet(),
            Executors.newScheduledThreadPool(4, runnable -> {
                Thread thread = new Thread(runnable);
                thread.setName("reactivesocket-scheduler-thread");
                thread.setDaemon(true);
                return thread;
            }),
            1, TimeUnit.SECONDS,
            10, TimeUnit.SECONDS,
            0.99,
            3, t -> true,
            null,
            null
        );
    }
}

