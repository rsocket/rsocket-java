package io.reactivesocket.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.reactivesocket.RequestHandler;
import io.reactivesocket.netty.tcp.server.ReactiveSocketServerHandler;

import java.util.List;

public class EchoServerHandler extends ByteToMessageDecoder {
    private static SimpleChannelInboundHandler<FullHttpRequest> httpHandler = new HttpServerHandler();

    private static ReactiveSocketServerHandler reactiveSocketHandler = ReactiveSocketServerHandler.create((setupPayload, rs) ->
        new RequestHandler.Builder().withRequestResponse(payload -> s -> {
            s.onNext(payload);
            s.onComplete();
        }).build());

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // Will use the first five bytes to detect a protocol.
        if (in.readableBytes() < 5) {
            return;
        }

        final int magic1 = in.getUnsignedByte(in.readerIndex());
        final int magic2 = in.getUnsignedByte(in.readerIndex() + 1);
        if (isHttp(magic1, magic2)) {
            switchToHttp(ctx);
        } else {
            switchToReactiveSocket(ctx);
        }
    }

    private static boolean isHttp(int magic1, int magic2) {
        return
            magic1 == 'G' && magic2 == 'E' || // GET
            magic1 == 'P' && magic2 == 'O' || // POST
            magic1 == 'P' && magic2 == 'U' || // PUT
            magic1 == 'H' && magic2 == 'E' || // HEAD
            magic1 == 'O' && magic2 == 'P' || // OPTIONS
            magic1 == 'P' && magic2 == 'A' || // PATCH
            magic1 == 'D' && magic2 == 'E' || // DELETE
            magic1 == 'T' && magic2 == 'R' || // TRACE
            magic1 == 'C' && magic2 == 'O';   // CONNECT
    }

    private void switchToHttp(ChannelHandlerContext ctx) {
        ChannelPipeline p = ctx.pipeline();
        p.addLast(new HttpServerCodec());
        p.addLast(new HttpObjectAggregator(256 * 1024));
        p.addLast(httpHandler);
        p.remove(this);
    }

    private void switchToReactiveSocket(ChannelHandlerContext ctx) {
        ChannelPipeline p = ctx.pipeline();
        p.addLast(reactiveSocketHandler);
        p.remove(this);
    }
}
