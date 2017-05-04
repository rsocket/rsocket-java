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

package io.rsocket.examples.transport.tcp.stream;

import io.rsocket.AbstractReactiveSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.ReactiveSocket;
import io.rsocket.client.ReactiveSocketClient;
import io.rsocket.lease.DisabledLeaseAcceptingSocket;
import io.rsocket.lease.LeaseEnforcingSocket;
import io.rsocket.server.ReactiveSocketServer;
import io.rsocket.server.ReactiveSocketServer.SocketAcceptor;
import io.rsocket.transport.TransportServer.StartedServer;
import io.rsocket.transport.netty.client.TcpTransportClient;
import io.rsocket.transport.netty.server.TcpTransportServer;
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

public final class StreamingClient {

    public static void main(String[] args) {
        StartedServer server = ReactiveSocketServer.create(TcpTransportServer.create(TcpServer.create()))
                                                   .start(new SocketAcceptorImpl());

        SocketAddress address = server.getServerAddress();
        ReactiveSocket socket = ReactiveSocketClient.create(TcpTransportClient.create(TcpClient.create(options ->
                        options.connect((InetSocketAddress)address))),
                                                                                   keepAlive(never()).disableLease())
                                                                           .connect()
                                        .block();

        socket.requestStream(new PayloadImpl("Hello"))
                .map(payload -> StandardCharsets.UTF_8.decode(payload.getData()).toString())
                .doOnNext(System.out::println)
                .take(10)
                .thenEmpty(socket.close())
                .block();
    }

    private static class SocketAcceptorImpl implements SocketAcceptor {
        @Override
        public LeaseEnforcingSocket accept(ConnectionSetupPayload setupPayload, ReactiveSocket reactiveSocket) {
            return new DisabledLeaseAcceptingSocket(new AbstractReactiveSocket() {
                @Override
                public Flux<Payload> requestStream(Payload payload) {
                    return Flux.interval(Duration.ofMillis(100))
                                   .map(aLong -> new PayloadImpl("Interval: " + aLong));
                }
            });
        }
    }
}
