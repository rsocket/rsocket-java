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

import io.rsocket.Plugins;
import io.rsocket.RSocket;
import io.rsocket.transport.TransportClient;
import reactor.core.publisher.Mono;

/**
 * Default implementation of {@link RSocketClient} providing the functionality to create a {@link RSocket}
 * from a {@link TransportClient}.
 */
public final class DefaultRSocketClient implements RSocketClient {

    private final Mono<RSocket> connectSource;

    public DefaultRSocketClient(TransportClient transportClient, SetupProvider setupProvider,
                                       SocketAcceptor acceptor) {
        connectSource =
            transportClient
                .connect()
                .then(connection -> setupProvider.accept(connection, acceptor))
                .then(Plugins.CLIENT_REACTIVE_SOCKET_INTERCEPTOR::apply);

    }

    @Override
    public Mono<? extends RSocket> connect() {
        return connectSource;
    }

    @Override
    public double availability() {
        return 1.0; // Client is always available unless wrapped with filters.
    }
}
