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

package io.reactivesocket.aeron.server;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.aeron.AeronDuplexConnection;
import io.reactivesocket.aeron.internal.AeronWrapper;
import io.reactivesocket.aeron.internal.EventLoop;
import io.reactivesocket.aeron.internal.reactivestreams.AeronChannelServer;
import io.reactivesocket.aeron.internal.reactivestreams.AeronSocketAddress;
import io.reactivesocket.aeron.internal.reactivestreams.ReactiveStreamsRemote;
import io.reactivesocket.transport.TransportServer;

import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class AeronTransportServer implements TransportServer {
    private final AeronWrapper aeronWrapper;
    private final AeronSocketAddress managementSubscriptionSocket;
    private final EventLoop eventLoop;

    private AeronChannelServer aeronChannelServer;

    public AeronTransportServer(AeronWrapper aeronWrapper, AeronSocketAddress managementSubscriptionSocket, EventLoop eventLoop) {
        this.aeronWrapper = aeronWrapper;
        this.managementSubscriptionSocket = managementSubscriptionSocket;
        this.eventLoop = eventLoop;
    }

    @Override
    public StartedServer start(ConnectionAcceptor acceptor) {
        synchronized (this) {
            if (aeronChannelServer != null) {
                throw new IllegalStateException("server already ready started");
            }

            aeronChannelServer = AeronChannelServer.create(
                aeronChannel -> {
                    DuplexConnection connection = new AeronDuplexConnection("server", aeronChannel);
                    acceptor.apply(connection).subscribe();
                },
                aeronWrapper,
                managementSubscriptionSocket,
                eventLoop);
        }

        final ReactiveStreamsRemote.StartedServer startedServer = aeronChannelServer.start();

        return new StartedServer() {
            @Override
            public SocketAddress getServerAddress() {
                return startedServer.getServerAddress();
            }

            @Override
            public int getServerPort() {
                return startedServer.getServerPort();
            }

            @Override
            public void awaitShutdown() {
                startedServer.awaitShutdown();
            }

            @Override
            public void awaitShutdown(long duration, TimeUnit durationUnit) {
                startedServer.awaitShutdown(duration, durationUnit);
            }

            @Override
            public void shutdown() {
                startedServer.shutdown();
            }
        };
    }

}
