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

package io.rsocket.examples.transport.tcp.duplex;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.client.RSocketClient;
import io.rsocket.client.RSocketClient.SocketAcceptor;
import io.rsocket.server.RSocketServer;
import io.rsocket.transport.ServerTransport.StartedServer;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.PayloadImpl;
import reactor.core.publisher.Flux;
import reactor.ipc.netty.tcp.TcpClient;
import reactor.ipc.netty.tcp.TcpServer;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static io.rsocket.client.KeepAliveProvider.*;
import static io.rsocket.client.SetupProvider.*;

public final class DuplexClient {

    public static void main(String[] args) {
        StartedServer server = RSocketServer.create(TcpServerTransport.create(TcpServer.create()))
                                                  .start((setupPayload, reactiveSocket) -> {
                                                      reactiveSocket.requestStream(new PayloadImpl("Hello-Bidi"))
                                                              .map(payload -> StandardCharsets.UTF_8.decode(payload.getData()).toString())
                                                              .log()
                                                              .subscribe();
                                                      return new DisabledLeaseAcceptingSocket(new AbstractRSocket() { });
                                                  });

        SocketAddress address = server.getServerAddress();

        RSocketClient rsclient = RSocketClient.createDuplex(TcpClientTransport.create(TcpClient.create(options ->
                options.connect((InetSocketAddress)address))), new SocketAcceptor() {
            @Override
            public LeaseEnforcingSocket accept(RSocket reactiveSocket) {
                return new DisabledLeaseAcceptingSocket(new AbstractRSocket() {
                    @Override
                    public Flux<Payload> requestStream(Payload payload) {
                        return Flux.interval(Duration.ofSeconds(1)).map(aLong -> new PayloadImpl("Bi-di Response => " + aLong));
                    }
                });
            }
        }, keepAlive(never()).disableLease());

        RSocket socket = rsclient.connect().block();

        socket.onClose().block();
    }
}
