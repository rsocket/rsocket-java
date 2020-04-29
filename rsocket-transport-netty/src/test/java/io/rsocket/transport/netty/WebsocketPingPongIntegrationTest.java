package io.rsocket.transport.netty;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.util.ReferenceCountUtil;
import io.rsocket.Closeable;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.rsocket.transport.netty.server.WebsocketRouteTransport;
import io.rsocket.transport.netty.server.WebsocketServerTransport;
import io.rsocket.util.DefaultPayload;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.server.HttpServer;
import reactor.test.StepVerifier;

public class WebsocketPingPongIntegrationTest {
  private static final String host = "localhost";
  private static final int port = 8088;

  private Closeable server;

  @AfterEach
  void tearDown() {
    server.dispose();
  }

  @ParameterizedTest
  @MethodSource("provideServerTransport")
  void webSocketPingPong(ServerTransport<Closeable> serverTransport) {
    server =
        RSocketServer.create(SocketAcceptor.forRequestResponse(Mono::just))
            .bind(serverTransport)
            .block();

    String expectedData = "data";
    String expectedPing = "ping";

    PingSender pingSender = new PingSender();

    HttpClient httpClient =
        HttpClient.create()
            .tcpConfiguration(
                tcpClient ->
                    tcpClient
                        .doOnConnected(b -> b.addHandlerLast(pingSender))
                        .host(host)
                        .port(port));

    RSocket rSocket =
        RSocketConnector.connectWith(WebsocketClientTransport.create(httpClient, "/")).block();

    rSocket
        .requestResponse(DefaultPayload.create(expectedData))
        .delaySubscription(pingSender.sendPing(expectedPing))
        .as(StepVerifier::create)
        .expectNextMatches(p -> expectedData.equals(p.getDataUtf8()))
        .expectComplete()
        .verify(Duration.ofSeconds(5));

    pingSender
        .receivePong()
        .as(StepVerifier::create)
        .expectNextMatches(expectedPing::equals)
        .expectComplete()
        .verify(Duration.ofSeconds(5));

    rSocket
        .requestResponse(DefaultPayload.create(expectedData))
        .delaySubscription(pingSender.sendPong())
        .as(StepVerifier::create)
        .expectNextMatches(p -> expectedData.equals(p.getDataUtf8()))
        .expectComplete()
        .verify(Duration.ofSeconds(5));
  }

  private static Stream<Arguments> provideServerTransport() {
    return Stream.of(
        Arguments.of(WebsocketServerTransport.create(host, port)),
        Arguments.of(
            new WebsocketRouteTransport(
                HttpServer.create().host(host).port(port), routes -> {}, "/")));
  }

  private static class PingSender extends ChannelInboundHandlerAdapter {
    private final MonoProcessor<Channel> channel = MonoProcessor.create();
    private final MonoProcessor<String> pong = MonoProcessor.create();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (msg instanceof PongWebSocketFrame) {
        pong.onNext(((PongWebSocketFrame) msg).content().toString(StandardCharsets.UTF_8));
        ReferenceCountUtil.safeRelease(msg);
        ctx.read();
      } else {
        super.channelRead(ctx, msg);
      }
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
      Channel ch = ctx.channel();
      if (!channel.isTerminated() && ch.isWritable()) {
        channel.onNext(ctx.channel());
      }
      super.channelWritabilityChanged(ctx);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
      Channel ch = ctx.channel();
      if (ch.isWritable()) {
        channel.onNext(ch);
      }
      super.handlerAdded(ctx);
    }

    public Mono<Void> sendPing(String data) {
      return send(
          new PingWebSocketFrame(Unpooled.wrappedBuffer(data.getBytes(StandardCharsets.UTF_8))));
    }

    public Mono<Void> sendPong() {
      return send(new PongWebSocketFrame());
    }

    public Mono<String> receivePong() {
      return pong;
    }

    private Mono<Void> send(WebSocketFrame webSocketFrame) {
      return channel.doOnNext(ch -> ch.writeAndFlush(webSocketFrame)).then();
    }
  }
}
