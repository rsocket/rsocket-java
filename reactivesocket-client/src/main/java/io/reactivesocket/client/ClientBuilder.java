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

import java.util.*;

public class ClientBuilder<T> {
    private final ReactiveSocketConnector<T> connector;
    private final Publisher<? extends Collection<T>> source;

    private ClientBuilder(
        ReactiveSocketConnector<T> connector,
        Publisher<? extends Collection<T>> source
    ) {
        this.connector = connector;
        this.source = source;
    }

    public ClientBuilder<T> withConnector(ReactiveSocketConnector<T> connector) {
        return new ClientBuilder<>(
            connector,
            source
        );
    }

    public ClientBuilder<T> withSource(Publisher<? extends Collection<T>> source) {
        return new ClientBuilder<>(
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
            .chain(DrainingSocket::new);

        Publisher<? extends Collection<ReactiveSocketFactory<T>>> factories =
            sourceToFactory(source, filterConnector);

        return new LoadBalancer<>(factories);
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
                        ReactiveSocketFactory<T> factory;
                        if ((factory = current.get(sa)) == null) {
                            next.put(sa, new FailureAwareFactory<>(connector.toFactory(sa)));
                        } else {
                            next.put(sa, factory);
                        }
                    }

                    current = next;
                    subscriber.onNext(current.values());
                }

                @Override
                public void onError(Throwable t) { subscriber.onError(t); }

                @Override
                public void onComplete() { subscriber.onComplete(); }
            });
    }

    public static <T> ClientBuilder<T> instance() {
        return new ClientBuilder<>(
            null,
            null
        );
    }
}

