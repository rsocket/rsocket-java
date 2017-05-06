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

package io.rsocket.server;

import io.rsocket.ClientRSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.FrameType;
import io.rsocket.Plugins;
import io.rsocket.RSocket;
import io.rsocket.ServerRSocket;
import io.rsocket.StreamIdSupplier;
import io.rsocket.client.KeepAliveProvider;
import io.rsocket.internal.ClientServerInputMultiplexer;
import io.rsocket.lease.DefaultLeaseHonoringSocket;
import io.rsocket.lease.LeaseHonoringSocket;
import io.rsocket.transport.TransportServer;
import io.rsocket.transport.TransportServer.StartedServer;
import java.util.function.Consumer;
import reactor.core.publisher.Mono;

public final class DefaultRSocketServer
        implements RSocketServer {

    private final TransportServer transportServer;
    private Consumer<Throwable> errorConsumer;

    public DefaultRSocketServer(TransportServer transportServer,
          Consumer<Throwable> errorConsumer) {
        this.transportServer = transportServer;
        this.errorConsumer = errorConsumer;
    }

    @Override
    public StartedServer start(SocketAcceptor acceptor) {
        return transportServer
            .start(connection -> {
            ClientServerInputMultiplexer multiplexer = new ClientServerInputMultiplexer(connection);
            return multiplexer
                    .asStreamZeroConnection()
                    .receive()
                    .next()
                    .then(setupFrame -> {
                        if (setupFrame.getType() == FrameType.SETUP) {
                            ConnectionSetupPayload setup = ConnectionSetupPayload.create(setupFrame);

                            return Mono.defer(() -> {
                                ClientRSocket clientRSocket = new ClientRSocket(multiplexer.asServerConnection(),
                                    Throwable::printStackTrace,
                                    StreamIdSupplier.serverSupplier(),
                                    KeepAliveProvider.never());

                                Mono<RSocket> wrappedClientRSocket =
                                    Plugins.CLIENT_REACTIVE_SOCKET_INTERCEPTOR.apply(clientRSocket);

                                return wrappedClientRSocket
                                    .then(sender -> {
                                        LeaseHonoringSocket lhs = new DefaultLeaseHonoringSocket(sender);
                                        clientRSocket.start(lhs);

                                        return Plugins.SERVER_REACTIVE_SOCKET_INTERCEPTOR.apply(acceptor.accept(setup, lhs));
                                    });

                            })
                            .then(handler -> {
                                ServerRSocket receiver = new ServerRSocket(multiplexer.asClientConnection(),
                                    handler,
                                    setup.willClientHonorLease(),
                                    errorConsumer);
                                receiver.start();
                                setupFrame.release();
                                return connection.onClose();

                            });

                        } else {
                            return Mono.<Void>error(new IllegalStateException("Invalid first frame on the connection: "
                                                                            + connection + ", frame type received: "
                                                                            + setupFrame.getType()));
                        }
                    });

        });
    }
}
