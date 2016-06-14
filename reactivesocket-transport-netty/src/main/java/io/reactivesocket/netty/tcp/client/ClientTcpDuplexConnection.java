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
package io.reactivesocket.netty.tcp.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.exceptions.TransportException;
import io.reactivesocket.rx.Completable;
import org.agrona.BitUtil;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.DirectProcessor;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;

public class ClientTcpDuplexConnection implements DuplexConnection {
    private final Channel channel;
    private final DirectProcessor<Frame> directProcessor;

    private ClientTcpDuplexConnection(Channel channel, DirectProcessor<Frame> directProcessor) {
        this.directProcessor = directProcessor;
        this.channel = channel;
    }

    public static Publisher<ClientTcpDuplexConnection> create(SocketAddress address, EventLoopGroup eventLoopGroup) {
        return s -> {
            DirectProcessor<Frame> directProcessor = DirectProcessor.create();
            ReactiveSocketClientHandler clientHandler = new ReactiveSocketClientHandler(directProcessor);
            Bootstrap bootstrap = new Bootstrap();
            ChannelFuture connect = bootstrap
                .group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.AUTO_READ, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(
                            new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE >> 1, 0, BitUtil.SIZE_OF_INT, -1 * BitUtil.SIZE_OF_INT, 0),
                            clientHandler
                        );
                    }
                }).connect(address);

            connect.addListener(connectFuture -> {
                if (connectFuture.isSuccess()) {
                    Channel ch = connect.channel();
                    s.onNext(new ClientTcpDuplexConnection(ch, directProcessor));
                    s.onComplete();
                } else {
                    s.onError(connectFuture.cause());
                }
            });
        };
    }

    @Override
    public final Publisher<Frame> getInput() {
        return directProcessor;
    }

    @Override
    public void addOutput(Publisher<Frame> o, Completable callback) {
        o.subscribe(new Subscriber<Frame>() {
            private Subscription subscription;

            @Override
            public void onSubscribe(Subscription s) {
                subscription = s;
                // TODO: wire back pressure
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(Frame frame) {
                try {
                    ByteBuf byteBuf = Unpooled.wrappedBuffer(frame.getByteBuffer());
                    ChannelFuture channelFuture = channel.writeAndFlush(byteBuf);
                    channelFuture.addListener(future -> {
                        Throwable cause = future.cause();
                        if (cause != null) {
                            if (cause instanceof ClosedChannelException) {
                                onError(new TransportException(cause));
                            } else {
                                onError(cause);
                            }
                        }
                    });
                } catch (Throwable t) {
                    onError(t);
                }
            }

            @Override
            public void onError(Throwable t) {
                callback.error(t);
                subscription.cancel();
            }

            @Override
            public void onComplete() {
                callback.success();
                subscription.cancel();
            }
        });
    }

    @Override
    public double availability() {
        return channel.isOpen() ? 1.0 : 0.0;
    }

    @Override
    public void close() throws IOException {
        channel.close();
        directProcessor.onComplete();
    }

    public String toString() {
        if (channel == null) {
            return "ClientTcpDuplexConnection(channel=null)";
        }

        return "ClientTcpDuplexConnection(channel=[" +
            "remoteAddress=" + channel.remoteAddress() + "," +
            "isActive=" + channel.isActive() + "," +
            "isOpen=" + channel.isOpen() + "," +
            "isRegistered=" + channel.isRegistered() + "," +
            "isWritable=" + channel.isWritable() + "," +
            "channelId=" + channel.id().asLongText() +
            "])";

    }
}
