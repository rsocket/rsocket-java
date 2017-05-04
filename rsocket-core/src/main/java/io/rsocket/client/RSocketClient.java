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

import io.rsocket.AbstractRSocket;
import io.rsocket.Availability;
import io.rsocket.RSocket;
import io.rsocket.lease.DisabledLeaseAcceptingSocket;
import io.rsocket.lease.LeaseEnforcingSocket;
import io.rsocket.transport.TransportClient;
import reactor.core.publisher.Mono;

public interface RSocketClient extends Availability {

    /**
     * Creates a new {@code RSocket} every time the returned {@code Publisher} is subscribed.
     *
     * @return A {@code Publisher} that provides a new {@code RSocket} every time it is subscribed.
     */
    Mono<? extends RSocket> connect();

    /**
     * Creates a new instances of {@code RSocketClient} using the passed {@code transportClient}. This client
     * will not accept any requests from the server, so the client is half duplex. To create full duplex clients use
     * {@link #createDuplex(TransportClient, SocketAcceptor, SetupProvider)}.
     *
     * @param transportClient Transport to use.
     * @param setupProvider provider of {@code RSocket} setup.
     *
     * @return A new {@code RSocketClient} client.
     */
    static RSocketClient create(TransportClient transportClient, SetupProvider setupProvider) {
        return createDuplex(transportClient, reactiveSocket -> {
            return new DisabledLeaseAcceptingSocket(new AbstractRSocket() { /*Reject all requests.*/ });
        }, setupProvider);
    }

    /**
     * Creates a new instances of {@code RSocketClient} using the passed {@code transportClient}. This creates a
     * full duplex client that accepts requests from the server once the connection is setup.
     *
     * @param transportClient Transport to use.
     * @param acceptor Acceptor to accept new {@link RSocket} established by the client.
     * @param setupProvider provider of {@code RSocket} setup.
     *
     * @return A new {@code RSocketClient} client.
     */
    static RSocketClient createDuplex(TransportClient transportClient, SocketAcceptor acceptor,
                                             SetupProvider setupProvider) {
        return new DefaultRSocketClient(transportClient, setupProvider, acceptor);
    }

    /**
     * {@code RSocket} is a full duplex protocol where a client and server are identical in terms of both having
     * the capability to initiate requests to their peer. This interface provides the contract where a client accepts
     * a new {@code RSocket} for sending requests to the peer and returns a new {@code RSocket} that will
     * be used to accept requests from it's peer.
     */
    interface SocketAcceptor {

        /**
         * Accepts the socket to send requests to the peer and returns another socket that accepts requests from the
         * peer.
         *
         * @param reactiveSocket Socket for sending requests to the peer.
         *
         * @return Socket for receiving requests from the peer.
         */
        LeaseEnforcingSocket accept(RSocket reactiveSocket);

    }
}
