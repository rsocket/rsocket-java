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
package io.reactivesocket.transport.tcp.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.reactivesocket.Frame;
import io.reactivesocket.transport.tcp.NettyDuplexConnection;
import io.reactivesocket.rx.Observer;
import org.agrona.BitUtil;
import org.reactivestreams.Publisher;

import java.net.SocketAddress;
import java.util.concurrent.CopyOnWriteArrayList;

public class ClientTcpDuplexConnection extends NettyDuplexConnection {
    private ClientTcpDuplexConnection(Channel channel, CopyOnWriteArrayList<Observer<Frame>> readers) {
        super(channel, readers);
    }

    public static Publisher<ClientTcpDuplexConnection> create(SocketAddress address, EventLoopGroup eventLoopGroup) {
        return s -> {
            CopyOnWriteArrayList<Observer<Frame>> readers = new CopyOnWriteArrayList<>();
            ReactiveSocketClientHandler clientHandler = new ReactiveSocketClientHandler(readers);
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
                    s.onNext(new ClientTcpDuplexConnection(ch, readers));
                    s.onComplete();
                } else {
                    s.onError(connectFuture.cause());
                }
            });
        };
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
