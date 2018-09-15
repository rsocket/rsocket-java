package io.rsocket.transport.netty;

import io.rsocket.ConnectionSetupPayload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.SocketAcceptor;
import io.rsocket.exceptions.RejectedSetupException;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.rsocket.transport.netty.server.NettyContextCloseable;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.transport.netty.server.WebsocketServerTransport;
import io.rsocket.util.DefaultPayload;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class SetupRejectionTest {

  @DisplayName(
      "Rejecting setup by server causes requester RSocket disposal and RejectedSetupException")
  @ParameterizedTest
  @MethodSource(value = "transports")
  void rejectSetupTcp(
      Function<InetSocketAddress, ServerTransport<NettyContextCloseable>> serverTransport,
      Function<InetSocketAddress, ClientTransport> clientTransport) {

    String errorMessage = "error";
    RejectingAcceptor acceptor = new RejectingAcceptor(errorMessage);
    Mono<RSocket> serverRequester = acceptor.requesterRSocket();

    NettyContextCloseable nettyCtx =
        RSocketFactory.receive()
            .acceptor(acceptor)
            .transport(serverTransport.apply(new InetSocketAddress(0)))
            .start()
            .block();

    ErrorConsumer errorConsumer = new ErrorConsumer();

    RSocket clientRequester =
        RSocketFactory.connect()
            .errorConsumer(errorConsumer)
            .transport(clientTransport.apply(nettyCtx.address()))
            .start()
            .block();

    StepVerifier.create(errorConsumer.errors().next())
        .expectNextMatches(
            err -> err instanceof RejectedSetupException && errorMessage.equals(err.getMessage()))
        .expectComplete()
        .verify(Duration.ofSeconds(5));

    StepVerifier.create(clientRequester.onClose()).expectComplete().verify(Duration.ofSeconds(5));
    StepVerifier.create(serverRequester.flatMap(RSocket::onClose))
        .expectComplete()
        .verify(Duration.ofSeconds(5));

    StepVerifier.create(clientRequester.requestResponse(DefaultPayload.create("test")))
        .expectErrorMatches(
            err -> err instanceof RejectedSetupException && errorMessage.equals(err.getMessage()))
        .verify(Duration.ofSeconds(5));

    nettyCtx.dispose();
  }

  static Stream<Arguments> transports() {
    Function<InetSocketAddress, ServerTransport<NettyContextCloseable>> tcpServer =
        TcpServerTransport::create;
    Function<InetSocketAddress, ServerTransport<NettyContextCloseable>> wsServer =
        WebsocketServerTransport::create;
    Function<InetSocketAddress, ClientTransport> tcpClient = TcpClientTransport::create;
    Function<InetSocketAddress, ClientTransport> wsClient = WebsocketClientTransport::create;

    return Stream.of(Arguments.of(tcpServer, tcpClient), Arguments.of(wsServer, wsClient));
  }

  static class ErrorConsumer implements Consumer<Throwable> {
    private final EmitterProcessor<Throwable> errors = EmitterProcessor.create();

    @Override
    public void accept(Throwable t) {
      errors.onNext(t);
    }

    Flux<Throwable> errors() {
      return errors;
    }
  }

  private static class RejectingAcceptor implements SocketAcceptor {
    private final String msg;
    private final EmitterProcessor<RSocket> requesters = EmitterProcessor.create();

    public RejectingAcceptor(String msg) {
      this.msg = msg;
    }

    @Override
    public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
      requesters.onNext(sendingSocket);
      return Mono.error(new RuntimeException(msg));
    }

    public Mono<RSocket> requesterRSocket() {
      return requesters.next();
    }
  }
}
