/*
 * Copyright 2017 Netflix, Inc.
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

package io.reactivesocket.server;

import io.reactivesocket.ClientReactiveSocket;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.FrameType;
import io.reactivesocket.ServerReactiveSocket;
import io.reactivesocket.StreamIdSupplier;
import io.reactivesocket.client.KeepAliveProvider;
import io.reactivesocket.events.AbstractEventSource;
import io.reactivesocket.events.ConnectionEventInterceptor;
import io.reactivesocket.events.ServerEventListener;
import io.reactivesocket.internal.ClientServerInputMultiplexer;
import io.reactivesocket.lease.DefaultLeaseHonoringSocket;
import io.reactivesocket.lease.LeaseEnforcingSocket;
import io.reactivesocket.lease.LeaseHonoringSocket;
import io.reactivesocket.transport.TransportServer;
import io.reactivesocket.transport.TransportServer.StartedServer;
import io.reactivesocket.util.Clock;
import reactor.core.publisher.Mono;

public final class DefaultReactiveSocketServer extends AbstractEventSource<ServerEventListener>
        implements ReactiveSocketServer {

    private final TransportServer transportServer;

    public DefaultReactiveSocketServer(TransportServer transportServer) {
        this.transportServer = transportServer;
    }

    @Override
    public StartedServer start(SocketAcceptor acceptor) {
        return transportServer.start(connection -> {
            DuplexConnection dc;
            if (isEventPublishingEnabled()) {
                long startTime = Clock.now();
                dc = new ConnectionEventInterceptor(connection, this);
                getEventListener().socketAccepted();
                dc.onClose().doFinally(signalType -> {
                    if (isEventPublishingEnabled()) {
                        getEventListener().socketClosed(Clock.elapsedSince(startTime), Clock.unit());
                    }
                }).subscribe();
            } else {
                dc = connection;
            }

            ClientServerInputMultiplexer multiplexer = new ClientServerInputMultiplexer(dc);

            return multiplexer
                    .asStreamZeroConnection()
                    .receive()
                    .next()
                    .then(setupFrame -> {
                        if (setupFrame.getType() == FrameType.SETUP) {
                            ConnectionSetupPayload setup = ConnectionSetupPayload.create(setupFrame);
                            ClientReactiveSocket sender = new ClientReactiveSocket(multiplexer.asServerConnection(),
                                                                                   Throwable::printStackTrace,
                                                                                   StreamIdSupplier.serverSupplier(),
                                                                                   KeepAliveProvider.never(),
                                                                                   this);
                            LeaseHonoringSocket lhs = new DefaultLeaseHonoringSocket(sender);
                            sender.start(lhs);
                            LeaseEnforcingSocket handler = acceptor.accept(setup, sender);
                            ServerReactiveSocket receiver = new ServerReactiveSocket(multiplexer.asClientConnection(),
                                                                                     handler,
                                                                                     setup.willClientHonorLease(),
                                                                                     Throwable::printStackTrace,
                                                                                     this);
                            receiver.start();
                            return dc.onClose();
                        } else {
                            return Mono.<Void>error(new IllegalStateException("Invalid first frame on the connection: "
                                                                            + dc + ", frame type received: "
                                                                            + setupFrame.getType()));
                        }
                    });
        });
    }
}
