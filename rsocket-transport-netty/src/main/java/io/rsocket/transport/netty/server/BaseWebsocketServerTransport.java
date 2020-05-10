package io.rsocket.transport.netty.server;

import static io.netty.channel.ChannelHandler.*;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.util.ReferenceCountUtil;
import io.rsocket.Closeable;
import io.rsocket.DuplexConnection;
import io.rsocket.fragmentation.FragmentationDuplexConnection;
import io.rsocket.fragmentation.ReassemblyDuplexConnection;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.WebsocketDuplexConnection;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.netty.Connection;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.websocket.WebsocketInbound;
import reactor.netty.http.websocket.WebsocketOutbound;

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

  /**
   * Creates a new Websocket handler
   *
   * @param acceptor the {@link ConnectionAcceptor} to use with the handler
   * @return a new Websocket handler
   * @throws NullPointerException if {@code acceptor} is {@code null}
   */
  public static BiFunction<WebsocketInbound, WebsocketOutbound, Publisher<Void>> newHandler(
      ConnectionAcceptor acceptor) {
    return newHandler(acceptor, 0);
  }

  /**
   * Creates a new Websocket handler
   *
   * @param acceptor the {@link ConnectionAcceptor} to use with the handler
   * @param mtu the fragment size
   * @return a new Websocket handler
   * @throws NullPointerException if {@code acceptor} is {@code null}
   */
  public static BiFunction<WebsocketInbound, WebsocketOutbound, Publisher<Void>> newHandler(
      ConnectionAcceptor acceptor, int mtu) {
    return (in, out) -> {
      DuplexConnection connection = new WebsocketDuplexConnection((Connection) in);
      if (mtu > 0) {
        connection = new FragmentationDuplexConnection(connection, mtu, false, "server");
      } else {
        connection = new ReassemblyDuplexConnection(connection, false);
      }
      return acceptor.apply(connection).then(out.neverComplete());
    };
  }
}
