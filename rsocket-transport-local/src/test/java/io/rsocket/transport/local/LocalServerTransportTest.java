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

package io.rsocket.transport.local;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

final class LocalServerTransportTest {

  @DisplayName("create throws NullPointerException with null name")
  @Test
  void createNullName() {
    assertThatNullPointerException()
        .isThrownBy(() -> LocalServerTransport.create(null))
        .withMessage("name must not be null");
  }

  @DisplayName("dispose removes name from registry")
  @Test
  void dispose() {
    LocalServerTransport.dispose("test-name");
  }

  @DisplayName("dispose throws NullPointerException with null name")
  @Test
  void disposeNullName() {
    assertThatNullPointerException()
        .isThrownBy(() -> LocalServerTransport.dispose(null))
        .withMessage("name must not be null");
  }

  @DisplayName("creates transports with ephemeral names")
  @Test
  void ephemeral() {
    LocalServerTransport serverTransport1 = LocalServerTransport.createEphemeral();
    LocalServerTransport serverTransport2 = LocalServerTransport.createEphemeral();

    assertThat(serverTransport1.getName()).isNotEqualTo(serverTransport2.getName());
  }

  @DisplayName("returns the server by name")
  @Test
  void findServer() {
    LocalServerTransport serverTransport = LocalServerTransport.createEphemeral();

    serverTransport
        .start(duplexConnection -> Mono.empty())
        .as(StepVerifier::create)
        .expectNextCount(1)
        .verifyComplete();

    assertThat(LocalServerTransport.findServer(serverTransport.getName())).isNotNull();
  }

  @DisplayName("returns null if server hasn't been started")
  @Test
  void findServerMissingName() {
    assertThat(LocalServerTransport.findServer("test-name")).isNull();
  }

  @DisplayName("findServer throws NullPointerException with null name")
  @Test
  void findServerNullName() {
    assertThatNullPointerException()
        .isThrownBy(() -> LocalServerTransport.findServer(null))
        .withMessage("name must not be null");
  }

  @DisplayName("creates transport with name")
  @Test
  void named() {
    LocalServerTransport serverTransport = LocalServerTransport.create("test-name");

    assertThat(serverTransport.getName()).isEqualTo("test-name");
  }

  @DisplayName("starts local server transport")
  @Test
  void start() {
    LocalServerTransport.createEphemeral()
        .start(duplexConnection -> Mono.empty())
        .as(StepVerifier::create)
        .expectNextCount(1)
        .verifyComplete();
  }

  @DisplayName("start throws NullPointerException with null acceptor")
  @Test
  void startNullAcceptor() {
    assertThatNullPointerException()
        .isThrownBy(() -> LocalServerTransport.createEphemeral().start(null))
        .withMessage("acceptor must not be null");
  }
}
