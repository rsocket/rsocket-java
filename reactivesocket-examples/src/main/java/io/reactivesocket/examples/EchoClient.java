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

import io.reactivesocket.ConnectionSetupHandler;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.RequestHandler;
import io.reactivesocket.client.ClientBuilder;
import io.reactivesocket.test.TestUtil;
import io.reactivesocket.transport.tcp.client.TcpReactiveSocketConnector;
import io.reactivesocket.transport.tcp.server.TcpReactiveSocketServer;
import io.reactivesocket.util.Unsafe;
import rx.Observable;
import rx.RxReactiveStreams;

import java.net.SocketAddress;
import java.util.Collections;

public final class EchoClient {

    public static void main(String... args) throws Exception {

        ConnectionSetupHandler setupHandler = (setupPayload, reactiveSocket) -> {
            return new RequestHandler.Builder()
                    .withRequestResponse(
                            payload -> RxReactiveStreams.toPublisher(Observable.just(payload)))
                    .build();
        };

        SocketAddress serverAddress = TcpReactiveSocketServer.create()
                                                             .start(setupHandler)
                                                             .getServerAddress();

        ConnectionSetupPayload setupPayload =
            ConnectionSetupPayload.create("UTF-8", "UTF-8", ConnectionSetupPayload.NO_FLAGS);

        TcpReactiveSocketConnector tcp = TcpReactiveSocketConnector.create(setupPayload, Throwable::printStackTrace);

        ReactiveSocket client = ClientBuilder.instance()
            .withSource(RxReactiveStreams.toPublisher(Observable.just(Collections.singletonList(serverAddress))))
            .withConnector(tcp)
            .build();

        Unsafe.awaitAvailability(client);

        Payload request = TestUtil.utf8EncodedPayload("Hello", "META");
        RxReactiveStreams.toObservable(client.requestResponse(request))
                         .map(TestUtil::dataAsString)
                         .toBlocking()
                         .forEach(System.out::println);
    }
}
