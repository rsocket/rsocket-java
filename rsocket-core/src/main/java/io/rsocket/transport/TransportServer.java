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

package io.rsocket.transport;

import io.rsocket.DuplexConnection;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * A server contract for writing transports of RSocket.
 */
public interface TransportServer {

    /**
     * Starts this server.
     *
     * @param acceptor An acceptor to process a newly accepted {@code DuplexConnection}
     *
     * @return A handle to retrieve information about a started server.
     */
    StartedServer start(ConnectionAcceptor acceptor);

    /**
     * A contract to accept a new {@code DuplexConnection}.
     */
    interface ConnectionAcceptor extends Function<DuplexConnection, Publisher<Void>> {

        /**
         * Accept a new {@code DuplexConnection} and returns {@code Publisher} signifying the end of processing of the
         * connection.
         *
         * @param duplexConnection New {@code DuplexConnection} to be processed.
         *
         * @return A {@code Publisher} which terminates when the processing of the connection finishes.
         */
        @Override
        Mono<Void> apply(DuplexConnection duplexConnection);
    }

    /**
     * A contract that represents a server that is started via {@link #start(ConnectionAcceptor)} method.
     */
    interface StartedServer {

        /**
         * Address for this server.
         *
         * @return Address for this server.
         */
        SocketAddress getServerAddress();

        /**
         * Port for this server.
         *
         * @return Port for this server.
         */
        int getServerPort();

        /**
         * Blocks till this server shutsdown. <p>
         *     <em>This does not shutdown the server.</em>
         */
        void awaitShutdown();

        /**
         * Blocks till this server shutsdown till the passed duration. <p>
         *     <em>This does not shutdown the server.</em>
         */
        void awaitShutdown(long duration, TimeUnit durationUnit);

        /**
         * Initiates the shutdown of this server.
         */
        void shutdown();
    }
}
