/*
 * Copyright 2015-2018 the original author or authors.
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

package io.rsocket.transport.netty.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.rsocket.AbstractRSocket;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Mono;
import reactor.netty.http.server.HttpServer;
import reactor.netty.tcp.TcpServer;
import reactor.test.StepVerifier;

final class TcpServerTransportTest {

  @DisplayName("creates server with BindAddress")
  @Test
  void createBindAddress() {
    assertThat(TcpServerTransport.create("test-bind-address", 8000)).isNotNull();
  }

  @DisplayName("creates server with InetSocketAddress")
  @Test
  void createInetSocketAddress() {
    assertThat(
            TcpServerTransport.create(
                InetSocketAddress.createUnresolved("test-bind-address", 8000)))
        .isNotNull();
  }

  @DisplayName("create throws NullPointerException with null bindAddress")
  @Test
  void createNullBindAddress() {
    assertThatNullPointerException()
        .isThrownBy(() -> TcpServerTransport.create(null, 8000))
        .withMessage("bindAddress must not be null");
  }

  @DisplayName("create throws NullPointerException with null address")
  @Test
  void createNullInetSocketAddress() {
    assertThatNullPointerException()
        .isThrownBy(() -> TcpServerTransport.create((InetSocketAddress) null))
        .withMessage("address must not be null");
  }

  @DisplayName("create throws NullPointerException with null server")
  @Test
  void createNullTcpClient() {
    assertThatNullPointerException()
        .isThrownBy(() -> TcpServerTransport.create((TcpServer) null))
        .withMessage("server must not be null");
  }

  @DisplayName("creates server with port")
  @Test
  void createPort() {
    assertThat(TcpServerTransport.create(8000)).isNotNull();
  }

  @DisplayName("creates client with TcpServer")
  @Test
  void createTcpClient() {
    assertThat(TcpServerTransport.create(TcpServer.create())).isNotNull();
  }

  @DisplayName("starts server")
  @Test
  void start() {
    InetSocketAddress address = InetSocketAddress.createUnresolved("localhost", 0);

    TcpServerTransport serverTransport = TcpServerTransport.create(address);

    serverTransport
        .start(duplexConnection -> Mono.empty(), 0)
        .as(StepVerifier::create)
        .expectNextCount(1)
        .verifyComplete();
  }

  @DisplayName("start throws NullPointerException with null acceptor")
  @Test
  void startNullAcceptor() {
    assertThatNullPointerException()
        .isThrownBy(() -> TcpServerTransport.create(8000).start(null, 0))
        .withMessage("acceptor must not be null");
  }

  @DisplayName("server dispose leads to connections close")
  @ParameterizedTest
  @MethodSource("transportProvider")
  void serverDisposeClosesConnections(Transports transports) {
    CloseableChannel server =
        RSocketFactory.receive()
            .acceptor((setup, rSocket) -> Mono.just(new AbstractRSocket() {}))
            .transport(transports.serverTransport)
            .start()
            .block();

    Mono<RSocket> clientMono =
        RSocketFactory.connect()
            .transport(transports.clientTransport.apply(server.address()))
            .start();
    RSocket client = clientMono.block();

    server.dispose();

    Assertions.assertTrue(server.isDisposed());
    server.onClose().as(StepVerifier::create).expectComplete().verify(Duration.ofSeconds(5));

    client.onClose().as(StepVerifier::create).expectComplete().verify(Duration.ofSeconds(5));

    clientMono
        .as(StepVerifier::create)
        .expectErrorMatches(
            err ->
                err instanceof ConnectException && err.getMessage().contains("Connection refused"))
        .verify(Duration.ofSeconds(5));
  }

  static Stream<Transports> transportProvider() {
    InetSocketAddress address = new InetSocketAddress("localhost", 0);

    TcpServerTransport tcpServerTransport = TcpServerTransport.create(address);
    WebsocketServerTransport wsServerTransport = WebsocketServerTransport.create(address);
    WebsocketRouteTransport wsServerRouteTransport =
        new WebsocketRouteTransport(
            HttpServer.create().host(address.getHostName()).port(address.getPort()),
            routes -> {},
            "/");

    return Stream.of(
        new Transports(tcpServerTransport, TcpClientTransport::create),
        new Transports(wsServerTransport, WebsocketClientTransport::create),
        new Transports(wsServerRouteTransport, WebsocketClientTransport::create));
  }

  private static class Transports {
    final ServerTransport<CloseableChannel> serverTransport;
    final Function<InetSocketAddress, ClientTransport> clientTransport;

    public Transports(
        ServerTransport<CloseableChannel> serverTransport,
        Function<InetSocketAddress, ClientTransport> clientTransport) {
      this.serverTransport = serverTransport;
      this.clientTransport = clientTransport;
    }
  }
}
