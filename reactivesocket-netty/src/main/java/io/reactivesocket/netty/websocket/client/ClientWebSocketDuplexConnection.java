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
package io.reactivesocket.netty.websocket.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.util.concurrent.Future;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.rx.Completable;
import io.reactivesocket.rx.Observable;
import io.reactivesocket.rx.Observer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.CopyOnWriteArrayList;

public class ClientWebSocketDuplexConnection implements DuplexConnection {
    private Channel channel;

    private Bootstrap bootstrap;

    private final CopyOnWriteArrayList<Observer<Frame>> subjects;

    private ClientWebSocketDuplexConnection(Channel channel, Bootstrap bootstrap,  CopyOnWriteArrayList<Observer<Frame>> subjects) {
        this.subjects  = subjects;
        this.channel = channel;
        this.bootstrap = bootstrap;
    }

    public static Publisher<ClientWebSocketDuplexConnection> create(InetSocketAddress address, EventLoopGroup eventLoopGroup) {
        return s -> {
            WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory.newHandshaker(
                URI.create("ws://" + address.getHostName() + ":" + address.getPort() + "/rs"),
                WebSocketVersion.V13,
                null,
                false,
                new DefaultHttpHeaders());

            CopyOnWriteArrayList<Observer<Frame>> subjects = new CopyOnWriteArrayList<>();
            ReactiveSocketClientHandler clientHandler = new ReactiveSocketClientHandler(subjects);
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
                }).connect(address);

            connect.addListener(connectFuture -> {
                if (connectFuture.isSuccess()) {
                    final Channel ch = connect.channel();
                    clientHandler
                        .getHandshakePromise()
                        .addListener(handshakeFuture -> {
                            if (handshakeFuture.isSuccess()) {
                                s.onNext(new ClientWebSocketDuplexConnection(ch, bootstrap, subjects));
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

    @Override
    public final Observable<Frame> getInput() {
        return o -> {
            o.onSubscribe(() -> subjects.removeIf(s -> s == o));
            subjects.add(o);
        };
    }

    @Override
    public void addOutput(Publisher<Frame> o, Completable callback) {
        o.subscribe(new Subscriber<Frame>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(Frame frame) {
                try {
                    ByteBuf byteBuf = Unpooled.wrappedBuffer(frame.getByteBuffer());
                    BinaryWebSocketFrame binaryWebSocketFrame = new BinaryWebSocketFrame(byteBuf);
                    ChannelFuture channelFuture = channel.writeAndFlush(binaryWebSocketFrame);
                    channelFuture.addListener((Future<? super Void> future) -> {
                        Throwable cause = future.cause();
                        if (cause != null) {
                            callback.error(cause);
                        }
                    });
                } catch (Throwable t) {
                    onError(t);
                }
            }

            @Override
            public void onError(Throwable t) {
                callback.error(t);
            }

            @Override
            public void onComplete() {
                callback.success();
            }
        });
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
