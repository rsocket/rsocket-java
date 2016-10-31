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

package io.reactivesocket.transport.tcp.client;

import io.netty.buffer.ByteBuf;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.transport.TransportClient;
import io.reactivesocket.transport.tcp.ReactiveSocketFrameCodec;
import io.reactivesocket.transport.tcp.ReactiveSocketFrameLogger;
import io.reactivesocket.transport.tcp.ReactiveSocketLengthCodec;
import io.reactivesocket.transport.tcp.TcpDuplexConnection;
import io.reactivex.netty.protocol.tcp.client.TcpClient;
import org.reactivestreams.Publisher;
import org.slf4j.event.Level;

import java.net.SocketAddress;
import java.util.function.Function;

import static rx.RxReactiveStreams.*;

public class TcpTransportClient implements TransportClient {

    private final TcpClient<Frame, Frame> rxNettyClient;

    public TcpTransportClient(TcpClient<Frame, Frame> client) {
        rxNettyClient = client;
    }

    @Override
    public Publisher<DuplexConnection> connect() {
        return toPublisher(rxNettyClient.createConnectionRequest()
                                        .map(connection -> new TcpDuplexConnection(connection)));
    }

    /**
     * Configures the underlying {@link TcpClient}.
     *
     * @param configurator Function to transform the client.
     *
     * @return A new {@link TcpTransportClient}
     */
    public TcpTransportClient configureClient(Function<TcpClient<Frame, Frame>, TcpClient<Frame, Frame>> configurator) {
        return new TcpTransportClient(configurator.apply(rxNettyClient));
    }

    /**
     * Enable logging of every frame read and written on every connection created by this client.
     *
     * @param name Name of the logger.
     * @param logLevel Level at which the messages will be logged.
     *
     * @return A new {@link TcpTransportClient}
     */
    public TcpTransportClient logReactiveSocketFrames(String name, Level logLevel) {
        return configureClient(c ->
            c.addChannelHandlerLast("reactive-socket-frame-codec", () -> new ReactiveSocketFrameLogger(name, logLevel))
        );
    }

    public static TcpTransportClient create(SocketAddress serverAddress) {
        return new TcpTransportClient(_configureClient(TcpClient.newClient(serverAddress)));
    }

    public static TcpTransportClient create(TcpClient<ByteBuf, ByteBuf> client) {
        return new TcpTransportClient(_configureClient(client));
    }

    private static TcpClient<Frame, Frame> _configureClient(TcpClient<ByteBuf, ByteBuf> client) {
        return client.addChannelHandlerLast("length-codec", ReactiveSocketLengthCodec::new)
                     .addChannelHandlerLast("frame-codec", ReactiveSocketFrameCodec::new);
    }
}
