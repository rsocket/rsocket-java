package io.rsocket.transport.netty.server;

import static io.netty.channel.ChannelHandler.*;
import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.util.ReferenceCountUtil;
import io.rsocket.Closeable;
import io.rsocket.transport.ServerTransport;
import java.util.function.Consumer;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.server.WebsocketServerSpec;

abstract class BaseWebsocketServerTransport<
        SELF extends BaseWebsocketServerTransport<SELF, T>, T extends Closeable>
    implements ServerTransport<T> {
  private static final Logger logger = LoggerFactory.getLogger(BaseWebsocketServerTransport.class);
  private static final ChannelHandler pongHandler = new PongHandler();

  static Function<HttpServer, HttpServer> serverConfigurer =
      server ->
          server.tcpConfiguration(
              tcpServer ->
                  tcpServer.doOnConnection(connection -> connection.addHandlerLast(pongHandler)));

  final WebsocketServerSpec.Builder specBuilder =
      WebsocketServerSpec.builder().maxFramePayloadLength(FRAME_LENGTH_MASK);

  /**
   * Provide a consumer to customize properties of the {@link WebsocketServerSpec} to use for
   * WebSocket upgrades. The consumer is invoked immediately.
   *
   * @param configurer the configurer to apply to the spec
   * @return the same instance for method chaining
   * @since 1.0.1
   */
  @SuppressWarnings("unchecked")
  public SELF webSocketSpec(Consumer<WebsocketServerSpec.Builder> configurer) {
    configurer.accept(specBuilder);
    return (SELF) this;
  }

  @Override
  public int maxFrameLength() {
    return specBuilder.build().maxFramePayloadLength();
  }

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
