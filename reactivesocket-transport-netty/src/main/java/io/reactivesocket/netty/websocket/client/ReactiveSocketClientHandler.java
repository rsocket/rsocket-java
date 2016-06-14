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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.reactivesocket.Frame;
import io.reactivesocket.netty.MutableDirectByteBuf;
import reactor.core.publisher.DirectProcessor;

@ChannelHandler.Sharable
public class ReactiveSocketClientHandler extends SimpleChannelInboundHandler<BinaryWebSocketFrame> {

    private final DirectProcessor<Frame> directProcessor;

    private ChannelPromise handshakePromise;

    public ReactiveSocketClientHandler(DirectProcessor<Frame> directProcessor) {
        this.directProcessor = directProcessor;
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        this.handshakePromise = ctx.newPromise();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, BinaryWebSocketFrame bFrame) throws Exception {
        ByteBuf content = bFrame.content();
        MutableDirectByteBuf mutableDirectByteBuf = new MutableDirectByteBuf(content);
        final Frame from = Frame.from(mutableDirectByteBuf, 0, mutableDirectByteBuf.capacity());
        directProcessor
            .onNext(from);
    }

    public ChannelPromise getHandshakePromise() {
        return handshakePromise;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof WebSocketClientProtocolHandler.ClientHandshakeStateEvent) {
            WebSocketClientProtocolHandler.ClientHandshakeStateEvent evt1 = (WebSocketClientProtocolHandler.ClientHandshakeStateEvent) evt;
            if (evt1.equals(WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE)) {
                handshakePromise.setSuccess();
            }
        }
    }
}
