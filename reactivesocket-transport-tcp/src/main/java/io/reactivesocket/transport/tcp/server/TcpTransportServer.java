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

package io.reactivesocket.transport.tcp.server;

import io.netty.buffer.ByteBuf;
import io.reactivesocket.Frame;
import io.reactivesocket.transport.TransportServer;
import io.reactivesocket.transport.tcp.ReactiveSocketFrameCodec;
import io.reactivesocket.transport.tcp.ReactiveSocketLengthCodec;
import io.reactivesocket.transport.tcp.TcpDuplexConnection;
import io.reactivex.netty.channel.Connection;
import io.reactivex.netty.protocol.tcp.server.ConnectionHandler;
import io.reactivex.netty.protocol.tcp.server.TcpServer;
import rx.Observable;

import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static rx.RxReactiveStreams.*;

public class TcpTransportServer implements TransportServer {

    private final TcpServer<Frame, Frame> rxNettyServer;

    private TcpTransportServer(TcpServer<Frame, Frame> rxNettyServer) {
        this.rxNettyServer = rxNettyServer;
    }

    @Override
    public StartedServer start(ConnectionAcceptor acceptor) {
        rxNettyServer.start(new ConnectionHandler<Frame, Frame>() {
            @Override
            public Observable<Void> handle(Connection<Frame, Frame> newConnection) {
                TcpDuplexConnection duplexConnection = new TcpDuplexConnection(newConnection);
                return toObservable(acceptor.apply(duplexConnection));
            }
        });
        return new Started();
    }

    /**
     * Configures the underlying server using the passed {@code configurator}.
     *
     * @param configurator Function to transform the underlying server.
     *
     * @return New instance of {@code TcpReactiveSocketServer}.
     */
    public TcpTransportServer configureServer(Function<TcpServer<Frame, Frame>, TcpServer<Frame, Frame>> configurator) {
        return new TcpTransportServer(configurator.apply(rxNettyServer));
    }

    public static TcpTransportServer create() {
        return create(TcpServer.newServer());
    }

    public static TcpTransportServer create(int port) {
        return create(TcpServer.newServer(port));
    }

    public static TcpTransportServer create(SocketAddress address) {
        return create(TcpServer.newServer(address));
    }

    public static TcpTransportServer create(TcpServer<ByteBuf, ByteBuf> rxNettyServer) {
        return new TcpTransportServer(configure(rxNettyServer));
    }

    private static TcpServer<Frame, Frame> configure(TcpServer<ByteBuf, ByteBuf> rxNettyServer) {
        return rxNettyServer.addChannelHandlerLast("line-codec", ReactiveSocketLengthCodec::new)
                            .addChannelHandlerLast("frame-codec", ReactiveSocketFrameCodec::new);
    }

    private class Started implements StartedServer {

        @Override
        public SocketAddress getServerAddress() {
            return rxNettyServer.getServerAddress();
        }

        @Override
        public int getServerPort() {
            return rxNettyServer.getServerPort();
        }

        @Override
        public void awaitShutdown() {
            rxNettyServer.awaitShutdown();
        }

        @Override
        public void awaitShutdown(long duration, TimeUnit durationUnit) {
            rxNettyServer.awaitShutdown(duration, durationUnit);
        }

        @Override
        public void shutdown() {
            rxNettyServer.shutdown();
        }
    }
}
