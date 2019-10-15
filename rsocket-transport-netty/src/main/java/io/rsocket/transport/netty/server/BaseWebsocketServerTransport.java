package io.rsocket.transport.netty.server;

import static io.netty.channel.ChannelHandler.*;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.util.ReferenceCountUtil;
import io.rsocket.Closeable;
import io.rsocket.transport.ServerTransport;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.netty.http.server.HttpServer;

abstract class BaseWebsocketServerTransport<T extends Closeable> implements ServerTransport<T> {
  private static final Logger logger = LoggerFactory.getLogger(BaseWebsocketServerTransport.class);
  private static final ChannelHandler pongHandler = new PongHandler();

  static Function<HttpServer, HttpServer> serverConfigurer =
      server ->
          server.tcpConfiguration(
              tcpServer ->
                  tcpServer.doOnConnection(connection -> connection.addHandlerLast(pongHandler)));

  @Sharable
  private static class PongHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      if (msg instanceof PongWebSocketFrame) {
        logger.debug("received WebSocket Pong Frame");
        ReferenceCountUtil.safeRelease(msg);
        ctx.read();
      } else {
        ctx.fireChannelRead(msg);
      }
    }
  }
}
