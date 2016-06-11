/**
 * Copyright 2015 Netflix, Inc.
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
package io.reactivesocket.transport.websocket.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.*;
import io.reactivesocket.Frame;
import io.reactivesocket.transport.tcp.NettyDuplexConnection;
import io.reactivesocket.rx.Observer;
import org.reactivestreams.Publisher;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CopyOnWriteArrayList;

public class ClientWebSocketDuplexConnection extends NettyDuplexConnection {

    private ClientWebSocketDuplexConnection(Channel channel, CopyOnWriteArrayList<Observer<Frame>> readers) {
        super(channel, readers);
    }

    public static Publisher<ClientWebSocketDuplexConnection> create(InetSocketAddress address, String path, EventLoopGroup eventLoopGroup) {
        try {
            return create(new URI("ws", null, address.getHostName(), address.getPort(), path, null, null), eventLoopGroup);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    public static Publisher<ClientWebSocketDuplexConnection> create(URI uri, EventLoopGroup eventLoopGroup) {
        return s -> {
            WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory.newHandshaker(
                    uri, WebSocketVersion.V13, null, false, new DefaultHttpHeaders());

            CopyOnWriteArrayList<Observer<Frame>> readers = new CopyOnWriteArrayList<>();
            ReactiveSocketClientHandler clientHandler = new ReactiveSocketClientHandler(readers);
            Bootstrap bootstrap = new Bootstrap();
            ChannelFuture connect = bootstrap
                .group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(
                            new HttpClientCodec(),
                            new HttpObjectAggregator(8192),
                            new WebSocketClientProtocolHandler(handshaker),
                            clientHandler
                        );
                    }
                }).connect(uri.getHost(), uri.getPort());

            connect.addListener(connectFuture -> {
                if (connectFuture.isSuccess()) {
                    final Channel ch = connect.channel();
                    clientHandler
                        .getHandshakePromise()
                        .addListener(handshakeFuture -> {
                            if (handshakeFuture.isSuccess()) {
                                s.onNext(new ClientWebSocketDuplexConnection(ch, readers));
                                s.onComplete();
                            } else {
                                s.onError(handshakeFuture.cause());
                            }
                        });
                } else {
                    s.onError(connectFuture.cause());
                }
            });
        };
    }

    public String toString() {
        if (channel == null) {
            return "ClientWebSocketDuplexConnection(channel=null)";
        }

        return "ClientWebSocketDuplexConnection(channel=["
            + "remoteAddress=" + channel.remoteAddress()
            + ", isActive=" + channel.isActive()
            + ", isOpen=" + channel.isOpen()
            + ", isRegistered=" + channel.isRegistered()
            + ", channelId=" + channel.id().asLongText()
            + "])";
    }
}
