/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket.examples;

import io.netty.channel.nio.NioEventLoopGroup;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.client.ClientBuilder;
import io.reactivesocket.transport.tcp.client.TcpReactiveSocketConnector;
import io.reactivesocket.util.Unsafe;
import io.reactivesocket.test.TestUtil;
import org.reactivestreams.Publisher;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class EchoClient {

    private static Publisher<List<SocketAddress>> source(SocketAddress sa) {
        return sub -> sub.onNext(Collections.singletonList(sa));
    }

    public static void main(String... args) throws Exception {
        InetSocketAddress address = InetSocketAddress.createUnresolved("localhost", 8888);
        ConnectionSetupPayload setupPayload =
            ConnectionSetupPayload.create("UTF-8", "UTF-8", ConnectionSetupPayload.NO_FLAGS);

        TcpReactiveSocketConnector tcp =
            new TcpReactiveSocketConnector(new NioEventLoopGroup(8), setupPayload, System.err::println);

        ReactiveSocket client = ClientBuilder.instance()
            .withSource(source(address))
            .withConnector(tcp)
            .build();

        Unsafe.awaitAvailability(client);

        Payload request = TestUtil.utf8EncodedPayload("Hello", "META");
        Payload response = Unsafe.blockingSingleWait(client.requestResponse(request), 1, TimeUnit.SECONDS);

        System.out.println(response);
    }
}
