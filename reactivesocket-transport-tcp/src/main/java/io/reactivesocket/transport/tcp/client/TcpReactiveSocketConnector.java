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
package io.reactivesocket.transport.tcp.client;

import io.netty.channel.EventLoopGroup;
import io.reactivesocket.*;
import io.reactivesocket.internal.rx.EmptySubscription;
import io.reactivesocket.rx.Completable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.net.SocketAddress;
import java.util.function.Consumer;

/**
 * An implementation of {@link ReactiveSocketConnector} that creates Netty TCP ReactiveSockets.
 */
public class TcpReactiveSocketConnector implements ReactiveSocketConnector<SocketAddress> {
    private final ConnectionSetupPayload connectionSetupPayload;
    private final Consumer<Throwable> errorStream;
    private final EventLoopGroup eventLoopGroup;

    public TcpReactiveSocketConnector(EventLoopGroup eventLoopGroup, ConnectionSetupPayload connectionSetupPayload, Consumer<Throwable> errorStream) {
        this.connectionSetupPayload = connectionSetupPayload;
        this.errorStream = errorStream;
        this.eventLoopGroup = eventLoopGroup;
    }

    @Override
    public Publisher<ReactiveSocket> connect(SocketAddress address) {
        Publisher<ClientTcpDuplexConnection> connection
            = ClientTcpDuplexConnection.create(address, eventLoopGroup);

        return subscriber -> connection.subscribe(new Subscriber<ClientTcpDuplexConnection>() {
            @Override
            public void onSubscribe(Subscription s) {
                subscriber.onSubscribe(s);
            }

            @Override
            public void onNext(ClientTcpDuplexConnection connection) {
                ReactiveSocket reactiveSocket = DefaultReactiveSocket.fromClientConnection(
                    connection, connectionSetupPayload, errorStream);
                reactiveSocket.start(new Completable() {
                    @Override
                    public void success() {
                        subscriber.onNext(reactiveSocket);
                        subscriber.onComplete();
                    }

                    @Override
                    public void error(Throwable e) {
                        subscriber.onError(e);
                    }
                });
            }

            @Override
            public void onError(Throwable t) {
                subscriber.onError(t);
            }

            @Override
            public void onComplete() {}
        });
    }
}
