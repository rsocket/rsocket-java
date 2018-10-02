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

package io.rsocket.transport.netty.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.rsocket.transport.netty.server.TcpServerTransport;
import java.net.InetSocketAddress;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.netty.tcp.TcpClient;
import reactor.test.StepVerifier;

final class TcpClientTransportTest {

  @DisplayName("connects to server")
  @Test
  void connect() {
    InetSocketAddress address = InetSocketAddress.createUnresolved("localhost", 0);

    TcpServerTransport serverTransport = TcpServerTransport.create(address);

    serverTransport
        .start(duplexConnection -> Mono.empty())
        .flatMap(context -> TcpClientTransport.create(context.address()).connect())
        .as(StepVerifier::create)
        .expectNextCount(1)
        .verifyComplete();
  }

  @DisplayName("create generates error if server not started")
  @Test
  void connectNoServer() {
    TcpClientTransport.create(8000).connect().as(StepVerifier::create).verifyError();
  }

  @DisplayName("creates client with BindAddress")
  @Test
  void createBindAddress() {
    assertThat(TcpClientTransport.create("test-bind-address", 8000)).isNotNull();
  }

  @DisplayName("creates client with InetSocketAddress")
  @Test
  void createInetSocketAddress() {
    assertThat(
            TcpClientTransport.create(
                InetSocketAddress.createUnresolved("test-bind-address", 8000)))
        .isNotNull();
  }

  @DisplayName("create throws NullPointerException with null bindAddress")
  @Test
  void createNullBindAddress() {
    assertThatNullPointerException()
        .isThrownBy(() -> TcpClientTransport.create(null, 8000))
        .withMessage("bindAddress must not be null");
  }

  @DisplayName("create throws NullPointerException with null address")
  @Test
  void createNullInetSocketAddress() {
    assertThatNullPointerException()
        .isThrownBy(() -> TcpClientTransport.create((InetSocketAddress) null))
        .withMessage("address must not be null");
  }

  @DisplayName("create throws NullPointerException with null client")
  @Test
  void createNullTcpClient() {
    assertThatNullPointerException()
        .isThrownBy(() -> TcpClientTransport.create((TcpClient) null))
        .withMessage("client must not be null");
  }

  @DisplayName("creates client with port")
  @Test
  void createPort() {
    assertThat(TcpClientTransport.create(8000)).isNotNull();
  }

  @DisplayName("creates client with TcpClient")
  @Test
  void createTcpClient() {
    assertThat(TcpClientTransport.create(TcpClient.create())).isNotNull();
  }
}
