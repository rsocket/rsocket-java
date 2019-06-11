package io.rsocket.transport.netty;

import static io.rsocket.RSocketFactory.*;

import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.SocketAcceptor;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.transport.netty.server.WebsocketServerTransport;
import java.time.Duration;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class RSocketFactoryNettyTransportFragmentationTest {

  @ParameterizedTest
  @MethodSource("serverTransportProvider")
  void serverErrorsWithEnabledFragmentationOnInsufficientMtu(
      ServerTransport<CloseableChannel> serverTransport) {
    Mono<CloseableChannel> server = createServer(serverTransport, f -> f.fragment(2));

    StepVerifier.create(server)
        .expectErrorMatches(
            err ->
                err instanceof IllegalArgumentException
                    && "smallest allowed mtu size is 64 bytes, provided: 2"
                        .equals(err.getMessage()))
        .verify(Duration.ofSeconds(5));
  }

  @ParameterizedTest
  @MethodSource("serverTransportProvider")
  void serverSucceedsWithEnabledFragmentationOnSufficientMtu(
      ServerTransport<CloseableChannel> serverTransport) {
    Mono<CloseableChannel> server =
        createServer(serverTransport, f -> f.fragment(100)).doOnNext(CloseableChannel::dispose);
    StepVerifier.create(server).expectNextCount(1).expectComplete().verify(Duration.ofSeconds(5));
  }

  @ParameterizedTest
  @MethodSource("serverTransportProvider")
  void serverSucceedsWithDisabledFragmentation() {
    Mono<CloseableChannel> server =
        createServer(TcpServerTransport.create("localhost", 0), Function.identity())
            .doOnNext(CloseableChannel::dispose);
    StepVerifier.create(server).expectNextCount(1).expectComplete().verify(Duration.ofSeconds(5));
  }

  @ParameterizedTest
  @MethodSource("serverTransportProvider")
  void clientErrorsWithEnabledFragmentationOnInsufficientMtu(
      ServerTransport<CloseableChannel> serverTransport) {
    CloseableChannel server = createServer(serverTransport, f -> f.fragment(100)).block();

    Mono<RSocket> rSocket =
        createClient(TcpClientTransport.create(server.address()), f -> f.fragment(2))
            .doFinally(s -> server.dispose());

    StepVerifier.create(rSocket)
        .expectErrorMatches(
            err ->
                err instanceof IllegalArgumentException
                    && "smallest allowed mtu size is 64 bytes, provided: 2"
                        .equals(err.getMessage()))
        .verify(Duration.ofSeconds(5));
  }

  @ParameterizedTest
  @MethodSource("serverTransportProvider")
  void clientSucceedsWithEnabledFragmentationOnSufficientMtu(
      ServerTransport<CloseableChannel> serverTransport) {
    CloseableChannel server = createServer(serverTransport, f -> f.fragment(100)).block();

    Mono<RSocket> rSocket =
        createClient(TcpClientTransport.create(server.address()), f -> f.fragment(100))
            .doFinally(s -> server.dispose());
    StepVerifier.create(rSocket).expectNextCount(1).expectComplete().verify(Duration.ofSeconds(5));
  }

  @ParameterizedTest
  @MethodSource("serverTransportProvider")
  void clientSucceedsWithDisabledFragmentation() {
    CloseableChannel server =
        createServer(TcpServerTransport.create("localhost", 0), Function.identity()).block();

    Mono<RSocket> rSocket =
        createClient(TcpClientTransport.create(server.address()), Function.identity())
            .doFinally(s -> server.dispose());
    StepVerifier.create(rSocket).expectNextCount(1).expectComplete().verify(Duration.ofSeconds(5));
  }

  private Mono<RSocket> createClient(
      ClientTransport transport, Function<ClientRSocketFactory, ClientRSocketFactory> f) {
    return f.apply(RSocketFactory.connect()).transport(transport).start();
  }

  private Mono<CloseableChannel> createServer(
      ServerTransport<CloseableChannel> transport,
      Function<ServerRSocketFactory, ServerRSocketFactory> f) {
    return f.apply(receive()).acceptor(mockAcceptor()).transport(transport).start();
  }

  private SocketAcceptor mockAcceptor() {
    SocketAcceptor mock = Mockito.mock(SocketAcceptor.class);
    Mockito.when(mock.accept(Mockito.any(), Mockito.any()))
        .thenReturn(Mono.just(Mockito.mock(RSocket.class)));
    return mock;
  }

  static Stream<? extends ServerTransport<CloseableChannel>> serverTransportProvider() {
    String host = "localhost";
    int port = 0;
    return Stream.of(
        TcpServerTransport.create(host, port), WebsocketServerTransport.create(host, port));
  }
}
