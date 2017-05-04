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

package io.rsocket.client;

import io.rsocket.RSocket;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * An implementation of {@code RSocketClient} that operates on a cluster of target servers instead of a single
 * server.
 */
public class LoadBalancingClient implements RSocketClient {

    private final LoadBalancerInitializer initializer;

    public LoadBalancingClient(LoadBalancerInitializer initializer) {
        this.initializer = initializer;
    }

    @Override
    public Mono<? extends RSocket> connect() {
        return initializer.connect();
    }

    @Override
    public double availability() {
        return initializer.availability();
    }

    /**
     * Creates a client that will load balance on the active servers as provided by the passed {@code servers}. A
     * server provided by this stream will be converted to a {@code RSocketClient} using the passed
     * {@code clientFactory}.
     *
     * @param servers Stream of a collection of active servers. Every emission on this stream must contain all active
     * servers. This client does not collect servers over multiple emissions.
     * @param clientFactory A function to convert a server to {@code RSocketClient}
     * @param <T> Type of the server.
     *
     * @return A new {@code LoadBalancingClient}.
     */
    public static <T> LoadBalancingClient create(Publisher<? extends Collection<T>> servers,
                                                 Function<T, RSocketClient> clientFactory) {
        SourceToClient<T> f = new SourceToClient<>(clientFactory);
        return new LoadBalancingClient(LoadBalancerInitializer.create(Flux.from(servers).map(f)));
    }

    /**
     * A mapping function from a collection of any type to a collection of {@code RSocketClient}.
     *
     * @param <T> Type of objects to convert to {@code RSocketClient}.
     */
    public static final class SourceToClient<T> implements Function<Collection<T>, Collection<RSocketClient>> {

        private final Function<T, RSocketClient> tToClient;
        private Map<T, RSocketClient> seenClients;

        public SourceToClient(Function<T, RSocketClient> tToClient) {
            this.tToClient = tToClient;
            seenClients = Collections.emptyMap();
        }

        @Override
        public Collection<RSocketClient> apply(Collection<T> servers) {
            Map<T, RSocketClient> next = new HashMap<>(servers.size());
            for (T server: servers) {
                RSocketClient client = seenClients.get(server);
                if (client == null) {
                    RSocketClient newClient = tToClient.apply(server);
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
