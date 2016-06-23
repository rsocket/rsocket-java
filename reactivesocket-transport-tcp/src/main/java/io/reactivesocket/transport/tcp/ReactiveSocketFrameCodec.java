/*
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
 *
 */

package io.reactivesocket.transport.tcp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import io.reactivesocket.Frame;

import java.nio.ByteBuffer;

/**
 * A Codec that aids reading and writing of ReactiveSocket {@link Frame}s.
 */
public class ReactiveSocketFrameCodec extends ChannelDuplexHandler {

    private final MutableDirectByteBuf buffer = new MutableDirectByteBuf(Unpooled.buffer(0));
    private final Frame frame = Frame.allocate(buffer);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            try {
                buffer.wrap((ByteBuf) msg);
                frame.wrap(buffer, 0);
                ctx.fireChannelRead(frame);
            } finally {
                ReferenceCountUtil.release(msg);
            }
        } else {
            super.channelRead(ctx, msg);
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof Frame) {
            ByteBuffer src = ((Frame)msg).getByteBuffer();
            ByteBuf toWrite = ctx.alloc().buffer(src.remaining()).writeBytes(src);
            ctx.write(toWrite, promise);
        } else {
            super.write(ctx, msg, promise);
        }
    }
}
