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
package io.reactivesocket.netty.tcp.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.rx.Completable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ServerTcpDuplexConnection implements DuplexConnection {
    private final DirectProcessor<Frame> directProcessor;

    private final ChannelHandlerContext ctx;

    public ServerTcpDuplexConnection(ChannelHandlerContext ctx) {
        this.directProcessor = DirectProcessor.create();
        this.ctx = ctx;
    }

    public DirectProcessor<Frame> getSubscribers() {
        return directProcessor;
    }

    @Override
    public final Publisher<Frame> getInput() {
        return directProcessor;
    }

    @Override
    public void addOutput(Publisher<Frame> o, Completable callback) {
        Flux
            .from(o)
            .doOnNext(frame -> {
                ByteBuffer data = frame.getByteBuffer();
                ByteBuf byteBuf = Unpooled.wrappedBuffer(data);
                ChannelFuture channelFuture = ctx.writeAndFlush(byteBuf);
                channelFuture.addListener(future -> {
                    Throwable cause = future.cause();
                    if (cause != null) {
                        cause.printStackTrace();
                        callback.error(cause);
                    }
                });
            })
            .doOnError(callback::error)
            .doOnComplete(callback::success)
            .subscribe();
    }

    @Override
    public double availability() {
        return ctx.channel().isOpen() ? 1.0 : 0.0;
    }

    @Override
    public void close() throws IOException {
        directProcessor.onComplete();
    }

    public String toString() {
        if (ctx ==null || ctx.channel() == null) {
            return  getClass().getName() + ":channel=null";
        }

        Channel channel = ctx.channel();
        return getClass().getName() + ":channel=[" +
            "remoteAddress=" + channel.remoteAddress() + "," +
            "isActive=" + channel.isActive() + "," +
            "isOpen=" + channel.isOpen() + "," +
            "isRegistered=" + channel.isRegistered() + "," +
            "isWritable=" + channel.isWritable() + "," +
            "channelId=" + channel.id().asLongText() +
            "]";

    }
}
