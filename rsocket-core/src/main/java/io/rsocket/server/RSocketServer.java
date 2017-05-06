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

package io.rsocket.server;

import io.rsocket.ConnectionSetupPayload;
import io.rsocket.RSocket;
import io.rsocket.exceptions.SetupException;
import io.rsocket.lease.LeaseEnforcingSocket;
import io.rsocket.transport.TransportServer;
import io.rsocket.transport.TransportServer.StartedServer;
import java.util.function.Consumer;

public interface RSocketServer {

    /**
     * Starts this server.
     *
     * @param acceptor Socket acceptor to use.
     *
     * @return Handle to get information about the started server.
     */
    StartedServer start(SocketAcceptor acceptor);

    static RSocketServer create(TransportServer transportServer) {
        return create(transportServer, Throwable::printStackTrace);
    }

    static RSocketServer create(TransportServer transportServer,
        Consumer<Throwable> errorConsumer) {
        return new DefaultRSocketServer(transportServer, errorConsumer);
    }

    /**
     * {@code RSocket} is a full duplex protocol where a client and server are identical in terms of both having
     * the capability to initiate requests to their peer. This interface provides the contract where a server accepts
     * a new {@code RSocket} for sending requests to the peer and returns a new {@code RSocket} that will
     * be used to accept requests from it's peer.
     */
    interface SocketAcceptor {

        /**
         * Accepts a new {@code RSocket} used to send requests to the peer and returns another
         * {@code RSocket} that is used for accepting requests from the peer.
         *
         * @param setup Setup as sent by the client.
         * @param sendingSocket Socket used to send requests to the peer.
         *
         * @return Socket to accept requests from the peer.
         *
         * @throws SetupException If the acceptor needs to reject the setup of this socket.
         */
        LeaseEnforcingSocket accept(ConnectionSetupPayload setup, RSocket sendingSocket);
    }
}
