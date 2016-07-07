/*
 * Copyright 2016 Netflix, Inc.
 * <p>
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *  <p>
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  <p>
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations under the License.
 */

package io.reactivesocket.local.server;

import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalServerChannel;
import io.reactivesocket.ConnectionSetupHandler;
import io.reactivesocket.Frame;
import io.reactivesocket.LeaseGovernor;
import io.reactivesocket.transport.tcp.server.TcpReactiveSocketServer;
import io.reactivesocket.transport.tcp.server.TcpReactiveSocketServer.StartedServer;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.protocol.tcp.server.TcpServer;

import java.util.function.Function;

/**
 * A server using local jvm sockets.
 */
public class LocalReactiveSocketServer {

    private final TcpReactiveSocketServer delegate;

    private LocalReactiveSocketServer(TcpReactiveSocketServer delegate) {
        this.delegate = delegate;
    }

    /**
     * Starts this server and uses the passed {@code setupHandler} to setup accepted connection.
     *
     * @param setupHandler Setup handler for connections.
     *
     * @return A handle for the started server.
     */
    public StartedServer start(ConnectionSetupHandler setupHandler) {
        return start(setupHandler, LeaseGovernor.UNLIMITED_LEASE_GOVERNOR);
    }

    /**
     * Starts this server and uses the passed {@code setupHandler} and {@code leaseGovernor} to setup accepted
     * connection.
     *
     * @param setupHandler Setup handler for connections.
     * @param leaseGovernor To manage leases.
     *
     * @return A handle for the started server.
     */
    public StartedServer start(ConnectionSetupHandler setupHandler, LeaseGovernor leaseGovernor) {
        return delegate.start(setupHandler, leaseGovernor);
    }

    /**
     * Configures the underlying server using the passed {@code configurator}.
     *
     * @param configurator Function to transform the underlying server.
     *
     * @return New instance of {@code LocalReactiveSocketServer}.
     */
    public LocalReactiveSocketServer configureServer(
            Function<TcpServer<Frame, Frame>, TcpServer<Frame, Frame>> configurator) {
        return new LocalReactiveSocketServer(delegate.configureServer(configurator));
    }

    /**
     * Creates a new local transport server with the passed {@code id}.
     *
     * @param id A unique identifier within the JVM.
     *
     * @return A new {@code {@link LocalReactiveSocketServer}
     */
    public static LocalReactiveSocketServer create(String id) {
        TcpReactiveSocketServer delegate =
                TcpReactiveSocketServer.create(TcpServer.newServer(new LocalAddress(id),
                                                                   RxNetty.getRxEventLoopProvider().globalServerEventLoop(),
                                                                   LocalServerChannel.class));
        return new LocalReactiveSocketServer(delegate);
    }
}
