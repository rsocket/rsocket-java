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

package io.reactivesocket.examples.transport.tcp.duplex;

import io.reactivesocket.AbstractReactiveSocket;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.client.ReactiveSocketClient;
import io.reactivesocket.client.ReactiveSocketClient.SocketAcceptor;
import io.reactivesocket.frame.ByteBufferUtil;
import io.reactivesocket.lease.DisabledLeaseAcceptingSocket;
import io.reactivesocket.lease.LeaseEnforcingSocket;
import io.reactivesocket.server.ReactiveSocketServer;
import io.reactivesocket.transport.TransportServer.StartedServer;
import io.reactivesocket.transport.tcp.client.TcpTransportClient;
import io.reactivesocket.transport.tcp.server.TcpTransportServer;
import io.reactivesocket.util.PayloadImpl;
import reactor.core.publisher.Flux;

import java.net.SocketAddress;
import java.time.Duration;

import static io.reactivesocket.client.KeepAliveProvider.*;
import static io.reactivesocket.client.SetupProvider.*;

public final class DuplexClient {

    public static void main(String[] args) {
        StartedServer server = ReactiveSocketServer.create(TcpTransportServer.create())
                                                  .start((setupPayload, reactiveSocket) -> {
                                                      reactiveSocket.requestStream(new PayloadImpl("Hello-Bidi"))
                                                              .map(Payload::getData)
                                                              .map(ByteBufferUtil::toUtf8String)
                                                              .log()
                                                              .subscribe();
                                                      return new DisabledLeaseAcceptingSocket(new AbstractReactiveSocket() { });
                                                  });

        SocketAddress address = server.getServerAddress();

        ReactiveSocketClient rsclient = ReactiveSocketClient.createDuplex(TcpTransportClient.create(address),
                                                                          new SocketAcceptor() {
            @Override
            public LeaseEnforcingSocket accept(ReactiveSocket reactiveSocket) {
                return new DisabledLeaseAcceptingSocket(new AbstractReactiveSocket() {
                    @Override
                    public Flux<Payload> requestStream(Payload payload) {
                        return Flux.interval(Duration.ofSeconds(1)).map(aLong -> new PayloadImpl("Bi-di Response => " + aLong));
                    }
                });
            }
        }, keepAlive(never()).disableLease());

        ReactiveSocket socket = rsclient.connect().block();

        socket.onClose().block();
    }
}
