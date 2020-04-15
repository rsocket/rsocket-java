package io.rsocket.transport.netty;

import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.transport.netty.server.WebsocketServerTransport;
import java.time.Duration;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class RSocketFactoryNettyTransportFragmentationTest {

  static Stream<? extends ServerTransport<CloseableChannel>> arguments() {
    return Stream.of(TcpServerTransport.create(0), WebsocketServerTransport.create(0));
  }

  @ParameterizedTest
  @MethodSource("arguments")
  void serverSucceedsWithEnabledFragmentationOnSufficientMtu(
      ServerTransport<CloseableChannel> serverTransport) {
    Mono<CloseableChannel> server =
        RSocketServer.create(mockAcceptor())
            .fragment(100)
            .bind(serverTransport)
            .doOnNext(CloseableChannel::dispose);
    StepVerifier.create(server).expectNextCount(1).expectComplete().verify(Duration.ofSeconds(5));
  }

  @ParameterizedTest
  @MethodSource("arguments")
  void serverSucceedsWithDisabledFragmentation(ServerTransport<CloseableChannel> serverTransport) {
    Mono<CloseableChannel> server =
        RSocketServer.create(mockAcceptor())
            .bind(serverTransport)
            .doOnNext(CloseableChannel::dispose);
    StepVerifier.create(server).expectNextCount(1).expectComplete().verify(Duration.ofSeconds(5));
  }

  @ParameterizedTest
  @MethodSource("arguments")
  void clientSucceedsWithEnabledFragmentationOnSufficientMtu(
      ServerTransport<CloseableChannel> serverTransport) {
    CloseableChannel server =
        RSocketServer.create(mockAcceptor()).fragment(100).bind(serverTransport).block();

    Mono<RSocket> rSocket =
        RSocketConnector.create()
            .fragment(100)
            .connect(TcpClientTransport.create(server.address()))
            .doFinally(s -> server.dispose());
    StepVerifier.create(rSocket).expectNextCount(1).expectComplete().verify(Duration.ofSeconds(5));
  }

  @ParameterizedTest
  @MethodSource("arguments")
  void clientSucceedsWithDisabledFragmentation(ServerTransport<CloseableChannel> serverTransport) {
    CloseableChannel server = RSocketServer.create(mockAcceptor()).bind(serverTransport).block();

    Mono<RSocket> rSocket =
        RSocketConnector.connectWith(TcpClientTransport.create(server.address()))
            .doFinally(s -> server.dispose());
    StepVerifier.create(rSocket).expectNextCount(1).expectComplete().verify(Duration.ofSeconds(5));
  }

  private SocketAcceptor mockAcceptor() {
    SocketAcceptor mock = Mockito.mock(SocketAcceptor.class);
    Mockito.when(mock.accept(Mockito.any(), Mockito.any()))
        .thenReturn(Mono.just(Mockito.mock(RSocket.class)));
    return mock;
  }
}
