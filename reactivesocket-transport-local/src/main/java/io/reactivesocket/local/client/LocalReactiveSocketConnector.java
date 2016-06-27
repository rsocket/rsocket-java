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

package io.reactivesocket.local.client;

import io.netty.channel.local.LocalChannel;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Frame;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.ReactiveSocketConnector;
import io.reactivesocket.transport.tcp.client.TcpReactiveSocketConnector;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.protocol.tcp.client.TcpClient;
import org.reactivestreams.Publisher;

import java.net.SocketAddress;
import java.util.function.Consumer;
import java.util.function.Function;

public class LocalReactiveSocketConnector implements ReactiveSocketConnector<SocketAddress> {

    private final TcpReactiveSocketConnector delegate;

    private LocalReactiveSocketConnector(TcpReactiveSocketConnector delegate) {
        this.delegate = delegate;
    }

    @Override
    public Publisher<ReactiveSocket> connect(SocketAddress address) {
        return delegate.connect(address);
    }
    /**
     * Configures the underlying {@link TcpClient} used by this connector.
     *
     * @param configurator Function to transform the client.
     *
     * @return A new {@link LocalReactiveSocketConnector}
     */
    public LocalReactiveSocketConnector configureClient(
            Function<TcpClient<Frame, Frame>, TcpClient<Frame, Frame>> configurator) {
        return new LocalReactiveSocketConnector(delegate.configureClient(configurator));
    }

    public static LocalReactiveSocketConnector create(ConnectionSetupPayload setupPayload,
                                                      Consumer<Throwable> errorStream) {
        TcpReactiveSocketConnector delegate =
                TcpReactiveSocketConnector.create(setupPayload, errorStream, socketAddress -> {
                    return TcpClient.newClient(socketAddress, RxNetty.getRxEventLoopProvider().globalClientEventLoop(),
                                               LocalChannel.class);
                });
        return new LocalReactiveSocketConnector(delegate);
    }
}
