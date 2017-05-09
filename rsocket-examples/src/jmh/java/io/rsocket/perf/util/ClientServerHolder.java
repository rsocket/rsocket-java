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

package io.rsocket.perf.util;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.client.KeepAliveProvider;
import io.rsocket.client.RSocketClient;
import io.rsocket.client.SetupProvider;
import io.rsocket.server.RSocketServer;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.ServerTransport.StartedServer;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.PayloadImpl;
import io.reactivex.Flowable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.tcp.TcpClient;
import reactor.ipc.netty.tcp.TcpServer;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

public class ClientServerHolder implements Supplier<RSocket> {

    public static final byte[] HELLO = "HELLO".getBytes(StandardCharsets.UTF_8);

    private final StartedServer server;
    private final RSocket client;

    public ClientServerHolder(ServerTransport serverTransport, Function<SocketAddress, ClientTransport> clientFactory,
                              RSocket handler) {
        server = startServer(serverTransport, handler);
        client = newClient(server.getServerAddress(), clientFactory);
    }

    @Override
    public RSocket get() {
        return client;
    }

    public static ClientServerHolder create(ServerTransport serverTransport,
                                            Function<SocketAddress, ClientTransport> clientFactory) {
        return new ClientServerHolder(serverTransport, clientFactory, new Handler());
    }

    public static Supplier<RSocket> requestResponseMultiTcp(int clientCount) {
        StartedServer server = startServer(TcpServerTransport.create(TcpServer.create()), new Handler());
        final RSocket[] sockets = new RSocket[clientCount];
        for (int i = 0; i < clientCount; i++) {
            sockets[i] = newClient(server.getServerAddress(), sock ->
                TcpClientTransport.create(TcpClient.create(options -> options.connect((InetSocketAddress)sock)))
            );
        }
        return new Supplier<RSocket>() {

            private final AtomicInteger index = new AtomicInteger();

            @Override
            public RSocket get() {
                int index = Math.abs(this.index.incrementAndGet()) % clientCount;
                return sockets[index];
            }
        };
    }

    private static StartedServer startServer(ServerTransport serverTransport, RSocket handler) {
        return RSocketServer.create(serverTransport)
                                   .start((setup, sendingSocket) -> {
                                       return new DisabledLeaseAcceptingSocket(handler);
                                   });
    }

    private static RSocket newClient(SocketAddress serverAddress,
                                            Function<SocketAddress, ClientTransport> clientFactory) {
        SetupProvider setupProvider = SetupProvider.keepAlive(KeepAliveProvider.never()).disableLease();
        RSocketClient client =
                RSocketClient.create(clientFactory.apply(serverAddress), setupProvider);
        return Flowable.fromPublisher(client.connect()).blockingLast();
    }

    private static class Handler extends AbstractRSocket {

        @Override
        public Mono<Payload> requestResponse(Payload payload) {
            return Mono.just(new PayloadImpl(HELLO));
        }

        @Override
        public Flux<Payload> requestStream(Payload payload) {
            return Flux.range(1, Integer.MAX_VALUE)
                           .map(integer -> new PayloadImpl(HELLO));
        }
    }
}
