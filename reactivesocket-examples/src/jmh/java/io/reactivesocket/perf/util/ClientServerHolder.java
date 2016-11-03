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
import io.reactivesocket.reactivestreams.extensions.Px;
import io.reactivesocket.server.ReactiveSocketServer;
import io.reactivesocket.transport.TransportClient;
import io.reactivesocket.transport.TransportServer;
import io.reactivesocket.transport.TransportServer.StartedServer;
import io.reactivesocket.transport.tcp.client.TcpTransportClient;
import io.reactivesocket.transport.tcp.server.TcpTransportServer;
import io.reactivesocket.util.PayloadImpl;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Publisher;

import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
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

    public static ClientServerHolder requestResponse(TransportServer transportServer,
                                                     Function<SocketAddress, TransportClient> clientFactory) {
        return new ClientServerHolder(transportServer, clientFactory, new RequestResponseHandler());
    }

    public static Supplier<ReactiveSocket> requestResponseMultiTcp(int clientCount) {
        StartedServer server = startServer(TcpTransportServer.create(), new RequestResponseHandler());
        final ReactiveSocket[] sockets = new ReactiveSocket[clientCount];
        for (int i = 0; i < clientCount; i++) {
            sockets[i] = newClient(server.getServerAddress(), sock -> TcpTransportClient.create(sock));
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

    private static class RequestResponseHandler extends AbstractReactiveSocket {
        @Override
        public Publisher<Payload> requestResponse(Payload payload) {
            return Px.just(new PayloadImpl(HELLO));
        }
    }
}
