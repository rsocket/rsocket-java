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

package io.reactivesocket.examples.transport.tcp.requestresponse;

import io.reactivesocket.AbstractReactiveSocket;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.client.ReactiveSocketClient;
import io.reactivesocket.exceptions.ApplicationException;
import io.reactivesocket.lease.DisabledLeaseAcceptingSocket;
import io.reactivesocket.server.ReactiveSocketServer;
import io.reactivesocket.transport.TransportServer.StartedServer;
import io.reactivesocket.transport.netty.client.TcpTransportClient;
import io.reactivesocket.transport.netty.server.TcpTransportServer;
import io.reactivesocket.util.PayloadImpl;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.tcp.TcpClient;
import reactor.ipc.netty.tcp.TcpServer;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;

import static io.reactivesocket.client.KeepAliveProvider.*;
import static io.reactivesocket.client.SetupProvider.*;

public final class HelloWorldClient {

    public static void main(String[] args) {
        BasicConfigurator.configure();
        Logger.getLogger("io.reactivesocket.FrameLogger").setLevel(Level.DEBUG);

        ReactiveSocketServer s = ReactiveSocketServer.create(TcpTransportServer.create(TcpServer.create()));
        StartedServer server = s.start((setupPayload, reactiveSocket) -> {
            return new DisabledLeaseAcceptingSocket(new AbstractReactiveSocket() {
                boolean first = true;
                @Override
                public Mono<Payload> requestResponse(Payload p) {
                    if (first) {
                        first = false;
                        return Mono.error(new ApplicationException(new PayloadImpl("my app error")));
                    } else {
                        return Mono.just(p);
                    }
                }
            });
        });

        SocketAddress address = server.getServerAddress();

        ReactiveSocketClient client = ReactiveSocketClient.create(TcpTransportClient.create(TcpClient.create(options ->
                        options.connect((InetSocketAddress)address))),
                                                                  keepAlive(never()).disableLease());
        ReactiveSocket socket = client.connect().block();

        System.out.println("First");

        try {
            socket.requestResponse(new PayloadImpl("Hello"))
                    .map(payload -> StandardCharsets.UTF_8.decode(payload.getData()).toString())
                    .doOnNext(System.out::println)
                    .block();
        } catch (Exception e) {
            System.err.println("Client handling error");
            e.printStackTrace();
        }

        System.out.println("Second");

        socket.requestResponse(new PayloadImpl("Hello"))
                .map(payload -> StandardCharsets.UTF_8.decode(payload.getData()).toString())
                .doOnNext(System.out::println)
                .doOnError(t -> {
                    System.err.println("Client handling error");
                    t.printStackTrace();
                })
                .block();

        socket.close().block();
    }
}
