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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.logging.LoggingHandler;
import io.reactivesocket.Frame;
import io.reactivesocket.transport.tcp.MutableDirectByteBuf;
import io.reactivesocket.rx.Observer;

import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;

@ChannelHandler.Sharable
public class ReactiveSocketClientHandler extends ChannelInboundHandlerAdapter {

    private final CopyOnWriteArrayList<Observer<Frame>> subjects;
    private final Logger logger;

    public ReactiveSocketClientHandler(CopyOnWriteArrayList<Observer<Frame>> subjects,
        Logger logger) {
        this.subjects = subjects;
        this.logger = logger;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object content) throws Exception {
        ByteBuf byteBuf = (ByteBuf) content;
        try {
            MutableDirectByteBuf mutableDirectByteBuf = new MutableDirectByteBuf(byteBuf);
            final Frame from = Frame.from(mutableDirectByteBuf, 0, mutableDirectByteBuf.capacity());

            if (logger.isDebugEnabled()) {
                logger.debug(ctx.channel().toString() + " RECEIVED: " + from);
            }
            subjects.forEach(o -> o.onNext(from));
        } finally {
            byteBuf.release();
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }
}
