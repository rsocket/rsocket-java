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
package io.reactivesocket.transport.tcp.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.reactivesocket.ConnectionSetupHandler;
import io.reactivesocket.DefaultReactiveSocket;
import io.reactivesocket.Frame;
import io.reactivesocket.LeaseGovernor;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.transport.tcp.MutableDirectByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.agrona.BitUtil.SIZE_OF_INT;

public class ReactiveSocketServerHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(ReactiveSocketServerHandler.class);
    private static final int MAX_FRAME_LENGTH = Integer.MAX_VALUE >> 1;

    private ConnectionSetupHandler setupHandler;
    private LeaseGovernor leaseGovernor;
    private ServerTcpDuplexConnection connection;

    protected ReactiveSocketServerHandler(ConnectionSetupHandler setupHandler, LeaseGovernor leaseGovernor) {
        this.setupHandler = setupHandler;
        this.leaseGovernor = leaseGovernor;
        this.connection = null;
    }

    public static ReactiveSocketServerHandler create(ConnectionSetupHandler setupHandler) {
        return create(setupHandler, LeaseGovernor.UNLIMITED_LEASE_GOVERNOR);
    }

    public static ReactiveSocketServerHandler create(ConnectionSetupHandler setupHandler, LeaseGovernor leaseGovernor) {
        return new ReactiveSocketServerHandler(setupHandler, leaseGovernor);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        ChannelPipeline cp = ctx.pipeline();
        if (cp.get(LengthFieldBasedFrameDecoder.class) == null) {
            LengthFieldBasedFrameDecoder frameDecoder =
                new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, SIZE_OF_INT, -1 * SIZE_OF_INT, 0);
            ctx.pipeline()
                .addBefore(ctx.name(), LengthFieldBasedFrameDecoder.class.getName(), frameDecoder);
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        connection = new ServerTcpDuplexConnection(ctx);
        ReactiveSocket reactiveSocket =
            DefaultReactiveSocket.fromServerConnection(connection, setupHandler, leaseGovernor, Throwable::printStackTrace);
        // Note: No blocking code here (still it should be refactored)
        reactiveSocket.startAndWait();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf content = (ByteBuf) msg;
        try {
            MutableDirectByteBuf mutableDirectByteBuf = new MutableDirectByteBuf(content);
            Frame from = Frame.from(mutableDirectByteBuf, 0, mutableDirectByteBuf.capacity());

            if (connection != null) {
                connection.getSubscribers().forEach(o -> o.onNext(from));
            }
        } finally {
            content.release();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);

        logger.error("caught an unhandled exception", cause);
    }
}
