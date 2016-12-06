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

import io.reactivesocket.AbstractReactiveSocket;
import io.reactivesocket.Availability;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.lease.DisabledLeaseAcceptingSocket;
import io.reactivesocket.lease.LeaseEnforcingSocket;
import io.reactivesocket.transport.TransportClient;
import org.reactivestreams.Publisher;

public interface ReactiveSocketClient extends Availability {

    /**
     * Creates a new {@code ReactiveSocket} every time the returned {@code Publisher} is subscribed.
     *
     * @return A {@code Publisher} that provides a new {@code ReactiveSocket} every time it is subscribed.
     */
    Publisher<? extends ReactiveSocket> connect();

    /**
     * Creates a new instances of {@code ReactiveSocketClient} using the passed {@code transportClient}. This client
     * will not accept any requests from the server, so the client is half duplex. To create full duplex clients use
     * {@link #createDuplex(TransportClient, SocketAcceptor, SetupProvider)}.
     *
     * @param transportClient Transport to use.
     * @param setupProvider provider of {@code ReactiveSocket} setup.
     *
     * @return A new {@code ReactiveSocketClient} client.
     */
    static ReactiveSocketClient create(TransportClient transportClient, SetupProvider setupProvider) {
        return createDuplex(transportClient, reactiveSocket -> {
            return new DisabledLeaseAcceptingSocket(new AbstractReactiveSocket() { /*Reject all requests.*/ });
        }, setupProvider);
    }

    /**
     * Creates a new instances of {@code ReactiveSocketClient} using the passed {@code transportClient}. This creates a
     * full duplex client that accepts requests from the server once the connection is setup.
     *
     * @param transportClient Transport to use.
     * @param acceptor Acceptor to accept new {@link ReactiveSocket} established by the client.
     * @param setupProvider provider of {@code ReactiveSocket} setup.
     *
     * @return A new {@code ReactiveSocketClient} client.
     */
    static ReactiveSocketClient createDuplex(TransportClient transportClient, SocketAcceptor acceptor,
                                             SetupProvider setupProvider) {
        return new DefaultReactiveSocketClient(transportClient, setupProvider, acceptor);
    }

    /**
     * {@code ReactiveSocket} is a full duplex protocol where a client and server are identical in terms of both having
     * the capability to initiate requests to their peer. This interface provides the contract where a client accepts
     * a new {@code ReactiveSocket} for sending requests to the peer and returns a new {@code ReactiveSocket} that will
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
        LeaseEnforcingSocket accept(ReactiveSocket reactiveSocket);

    }
}
