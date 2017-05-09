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

package io.rsocket.examples.transport.tcp.requestresponse;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.client.RSocketClient;
import io.rsocket.client.SetupProvider;
import io.rsocket.server.RSocketServer;
import io.rsocket.transport.ServerTransport.StartedServer;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.PayloadImpl;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.tcp.TcpClient;
import reactor.ipc.netty.tcp.TcpServer;

import static io.rsocket.client.KeepAliveProvider.never;
import static io.rsocket.client.SetupProvider.keepAlive;

public final class HelloWorldClient {

    public static void main(String[] args) {
        RSocketServer s = RSocketServer.create(
            TcpServerTransport.create(TcpServer.create()),
            e -> {
                System.err.println("Server received error");
                e.printStackTrace();
            });

        StartedServer server = s.start((setupPayload, reactiveSocket) -> {
            return new DisabledLeaseAcceptingSocket(new AbstractRSocket() {
                boolean fail = true;
                @Override
                public Mono<Payload> requestResponse(Payload p) {
                    if (fail) {
                        fail = false;
                        return Mono.error(new Throwable());
                    } else {
                        return Mono.just(p);
                    }
                }
            });
        });

        SocketAddress address = server.getServerAddress();
        SetupProvider setupProvider = keepAlive(never()).disableLease().errorConsumer(e -> {
            System.err.println("Client received error");
            e.printStackTrace();
        });
        RSocketClient client =
            RSocketClient.create(TcpClientTransport.create(TcpClient.create(options ->
                    options.connect((InetSocketAddress) address))),
                setupProvider);
        RSocket socket = client.connect().block();

        socket.requestResponse(new PayloadImpl("Hello"))
                .map(payload -> StandardCharsets.UTF_8.decode(payload.getData()).toString())
                .onErrorReturn("error")
                .doOnNext(System.out::println)
                .block();

        socket.requestResponse(new PayloadImpl("Hello"))
                .map(payload -> StandardCharsets.UTF_8.decode(payload.getData()).toString())
                .onErrorReturn("error")
                .doOnNext(System.out::println)
                .block();

        socket.requestResponse(new PayloadImpl("Hello"))
                .map(payload -> StandardCharsets.UTF_8.decode(payload.getData()).toString())
                .onErrorReturn("error")
                .doOnNext(System.out::println)
                .block();

        socket.close().block();
    }
}
