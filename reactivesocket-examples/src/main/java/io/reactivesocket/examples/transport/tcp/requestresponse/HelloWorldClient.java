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
import io.reactivesocket.frame.ByteBufferUtil;
import io.reactivesocket.lease.DisabledLeaseAcceptingSocket;
import io.reactivesocket.server.ReactiveSocketServer;
import io.reactivesocket.transport.TransportServer.StartedServer;
import io.reactivesocket.transport.tcp.client.TcpTransportClient;
import io.reactivesocket.transport.tcp.server.TcpTransportServer;
import io.reactivesocket.util.PayloadImpl;
import io.reactivex.Flowable;
import org.reactivestreams.Publisher;

import java.net.SocketAddress;

import static io.reactivesocket.client.KeepAliveProvider.*;
import static io.reactivesocket.client.SetupProvider.*;

public final class HelloWorldClient {

    public static void main(String[] args) {

        ReactiveSocketServer s = ReactiveSocketServer.create(TcpTransportServer.create());
        StartedServer server = s.start((setupPayload, reactiveSocket) -> {
            return new DisabledLeaseAcceptingSocket(new AbstractReactiveSocket() {
                @Override
                public Publisher<Payload> requestResponse(Payload p) {
                    return Flowable.just(p);
                }
            });
        });

        SocketAddress address = server.getServerAddress();
        ReactiveSocketClient client = ReactiveSocketClient.create(TcpTransportClient.create(address),
                                                                  keepAlive(never()).disableLease());
        ReactiveSocket socket = Flowable.fromPublisher(client.connect()).singleOrError().blockingGet();

        Flowable.fromPublisher(socket.requestResponse(new PayloadImpl("Hello")))
                .map(payload -> payload.getData())
                .map(ByteBufferUtil::toUtf8String)
                .doOnNext(System.out::println)
                .concatWith(Flowable.fromPublisher(socket.close()).cast(String.class))
                .ignoreElements()
                .blockingAwait();
    }
}
