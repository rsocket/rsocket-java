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

package io.reactivesocket.client;

import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.reactivestreams.extensions.Px;
import org.reactivestreams.Publisher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * An implementation of {@code ReactiveSocketClient} that operates on a cluster of target servers instead of a single
 * server.
 */
public class LoadBalancingClient extends AbstractReactiveSocketClient {

    private final LoadBalancerInitializer initializer;

    public LoadBalancingClient(LoadBalancerInitializer initializer) {
        super(initializer);
        this.initializer = initializer;
    }

    @Override
    public Publisher<? extends ReactiveSocket> connect() {
        return initializer.connect();
    }

    @Override
    public double availability() {
        return initializer.availability();
    }

    /**
     * Creates a client that will load balance on the active servers as provided by the passed {@code servers}. A
     * server provided by this stream will be converted to a {@code ReactiveSocketClient} using the passed
     * {@code clientFactory}.
     *
     * @param servers Stream of a collection of active servers. Every emission on this stream must contain all active
     * servers. This client does not collect servers over multiple emissions.
     * @param clientFactory A function to convert a server to {@code ReactiveSocketClient}
     * @param <T> Type of the server.
     *
     * @return A new {@code LoadBalancingClient}.
     */
    public static <T> LoadBalancingClient create(Publisher<? extends Collection<T>> servers,
                                                 Function<T, ReactiveSocketClient> clientFactory) {
        SourceToClient<T> f = new SourceToClient<T>(clientFactory);
        return new LoadBalancingClient(LoadBalancerInitializer.create(Px.from(servers).map(f)));
    }

    /**
     * A mapping function from a collection of any type to a collection of {@code ReactiveSocketClient}.
     *
     * @param <T> Type of objects to convert to {@code ReactiveSocketClient}.
     */
    public static final class SourceToClient<T> implements Function<Collection<T>, Collection<ReactiveSocketClient>> {

        private final Function<T, ReactiveSocketClient> tToClient;
        private Map<T, ReactiveSocketClient> seenClients;

        public SourceToClient(Function<T, ReactiveSocketClient> tToClient) {
            this.tToClient = tToClient;
            seenClients = Collections.emptyMap();
        }

        @Override
        public Collection<ReactiveSocketClient> apply(Collection<T> servers) {
            Map<T, ReactiveSocketClient> next = new HashMap<>(servers.size());
            for (T server: servers) {
                ReactiveSocketClient client = seenClients.get(server);
                if (client == null) {
                    ReactiveSocketClient newClient = tToClient.apply(server);
                    next.put(server, newClient);
                } else {
                    next.put(server, client);
                }
            }
            seenClients.clear();
            seenClients = next;
            return new ArrayList<>(seenClients.values());
        }
    }
}
