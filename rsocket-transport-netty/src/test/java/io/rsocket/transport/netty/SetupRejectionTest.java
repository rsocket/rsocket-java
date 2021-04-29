/*
 * Copyright 2015-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rsocket.transport.netty;

import io.rsocket.ConnectionSetupPayload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.exceptions.RejectedSetupException;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.transport.netty.server.WebsocketServerTransport;
import io.rsocket.util.DefaultPayload;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

public class SetupRejectionTest {

  /*
  TODO Fix this test
  @DisplayName(
      "Rejecting setup by server causes requester RSocket disposal and RejectedSetupException")
  @ParameterizedTest
  @MethodSource(value = "transports")*/
  void rejectSetupTcp(
      Function<InetSocketAddress, ServerTransport<CloseableChannel>> serverTransport,
      Function<InetSocketAddress, ClientTransport> clientTransport) {

    String errorMessage = "error";
    RejectingAcceptor acceptor = new RejectingAcceptor(errorMessage);
    Mono<RSocket> serverRequester = acceptor.requesterRSocket();

    CloseableChannel channel =
        RSocketServer.create(acceptor)
            .bind(serverTransport.apply(new InetSocketAddress("localhost", 0)))
            .block(Duration.ofSeconds(5));

    ErrorConsumer errorConsumer = new ErrorConsumer();

    RSocket clientRequester =
        RSocketConnector.connectWith(clientTransport.apply(channel.address()))
            .doOnError(errorConsumer)
            .block(Duration.ofSeconds(5));

    StepVerifier.create(errorConsumer.errors().next())
        .expectNextMatches(
            err -> err instanceof RejectedSetupException && errorMessage.equals(err.getMessage()))
        .expectComplete()
        .verify(Duration.ofSeconds(5));

    StepVerifier.create(clientRequester.onClose()).expectComplete().verify(Duration.ofSeconds(5));

    StepVerifier.create(serverRequester.flatMap(socket -> socket.onClose()))
        .expectComplete()
        .verify(Duration.ofSeconds(5));

    StepVerifier.create(clientRequester.requestResponse(DefaultPayload.create("test")))
        .expectErrorMatches(
            err -> err instanceof RejectedSetupException && errorMessage.equals(err.getMessage()))
        .verify(Duration.ofSeconds(5));

    channel.dispose();
  }

  static Stream<Arguments> transports() {
    Function<InetSocketAddress, ServerTransport<CloseableChannel>> tcpServer =
        TcpServerTransport::create;
    Function<InetSocketAddress, ServerTransport<CloseableChannel>> wsServer =
        WebsocketServerTransport::create;
    Function<InetSocketAddress, ClientTransport> tcpClient = TcpClientTransport::create;
    Function<InetSocketAddress, ClientTransport> wsClient = WebsocketClientTransport::create;

    return Stream.of(Arguments.of(tcpServer, tcpClient), Arguments.of(wsServer, wsClient));
  }

  static class ErrorConsumer implements Consumer<Throwable> {
    private final Sinks.Many<Throwable> errors = Sinks.many().multicast().onBackpressureBuffer();

    @Override
    public void accept(Throwable t) {
      errors.tryEmitNext(t);
    }

    Flux<Throwable> errors() {
      return errors.asFlux();
    }
  }

  private static class RejectingAcceptor implements SocketAcceptor {
    private final String msg;
    private final Sinks.Many<RSocket> requesters = Sinks.many().multicast().onBackpressureBuffer();

    public RejectingAcceptor(String msg) {
      this.msg = msg;
    }

    @Override
    public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
      requesters.tryEmitNext(sendingSocket);
      return Mono.error(new RuntimeException(msg));
    }

    public Mono<RSocket> requesterRSocket() {
      return requesters.asFlux().next();
    }
  }
}
