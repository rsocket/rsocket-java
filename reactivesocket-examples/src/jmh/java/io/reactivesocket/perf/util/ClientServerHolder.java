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

package io.reactivesocket.perf.util;

import io.reactivesocket.AbstractReactiveSocket;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.client.KeepAliveProvider;
import io.reactivesocket.client.ReactiveSocketClient;
import io.reactivesocket.client.SetupProvider;
import io.reactivesocket.lease.DisabledLeaseAcceptingSocket;
import io.reactivesocket.server.ReactiveSocketServer;
import io.reactivesocket.transport.TransportClient;
import io.reactivesocket.transport.TransportServer;
import io.reactivesocket.transport.TransportServer.StartedServer;
import io.reactivesocket.transport.netty.client.TcpTransportClient;
import io.reactivesocket.transport.netty.server.TcpTransportServer;
import io.reactivesocket.util.PayloadImpl;
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

public class ClientServerHolder implements Supplier<ReactiveSocket> {

    public static final byte[] HELLO = "HELLO".getBytes(StandardCharsets.UTF_8);

    private final StartedServer server;
    private final ReactiveSocket client;

    public ClientServerHolder(TransportServer transportServer, Function<SocketAddress, TransportClient> clientFactory,
                              ReactiveSocket handler) {
        server = startServer(transportServer, handler);
        client = newClient(server.getServerAddress(), clientFactory);
    }

    @Override
    public ReactiveSocket get() {
        return client;
    }

    public static ClientServerHolder create(TransportServer transportServer,
                                            Function<SocketAddress, TransportClient> clientFactory) {
        return new ClientServerHolder(transportServer, clientFactory, new Handler());
    }

    public static Supplier<ReactiveSocket> requestResponseMultiTcp(int clientCount) {
        StartedServer server = startServer(TcpTransportServer.create(TcpServer.create()), new Handler());
        final ReactiveSocket[] sockets = new ReactiveSocket[clientCount];
        for (int i = 0; i < clientCount; i++) {
            sockets[i] = newClient(server.getServerAddress(), sock ->
                TcpTransportClient.create(TcpClient.create(options -> options.connect((InetSocketAddress)sock)))
            );
        }
        return new Supplier<ReactiveSocket>() {

            private final AtomicInteger index = new AtomicInteger();

            @Override
            public ReactiveSocket get() {
                int index = Math.abs(this.index.incrementAndGet()) % clientCount;
                return sockets[index];
            }
        };
    }

    private static StartedServer startServer(TransportServer transportServer, ReactiveSocket handler) {
        return ReactiveSocketServer.create(transportServer)
                                   .start((setup, sendingSocket) -> {
                                       return new DisabledLeaseAcceptingSocket(handler);
                                   });
    }

    private static ReactiveSocket newClient(SocketAddress serverAddress,
                                            Function<SocketAddress, TransportClient> clientFactory) {
        SetupProvider setupProvider = SetupProvider.keepAlive(KeepAliveProvider.never()).disableLease();
        ReactiveSocketClient client =
                ReactiveSocketClient.create(clientFactory.apply(serverAddress), setupProvider);
        return Flowable.fromPublisher(client.connect()).blockingLast();
    }

    private static class Handler extends AbstractReactiveSocket {

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
