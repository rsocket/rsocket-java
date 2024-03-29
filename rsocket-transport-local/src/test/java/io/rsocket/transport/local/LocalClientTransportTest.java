/*
 * Copyright 2015-2020 the original author or authors.
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

import io.rsocket.Closeable;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

final class LocalClientTransportTest {

  @DisplayName("connects to server")
  @Test
  void connect() {
    LocalServerTransport serverTransport = LocalServerTransport.createEphemeral();

    Closeable closeable =
        serverTransport.start(duplexConnection -> duplexConnection.receive().then()).block();

    try {
      LocalClientTransport.create(serverTransport.getName())
          .connect()
          .doOnNext(d -> d.receive().subscribe())
          .as(StepVerifier::create)
          .expectNextCount(1)
          .verifyComplete();
    } finally {
      closeable.dispose();
      closeable.onClose().block(Duration.ofSeconds(5));
    }
  }

  @DisplayName("generates error if server not started")
  @Test
  void connectNoServer() {
    LocalClientTransport.create("test-name")
        .connect()
        .as(StepVerifier::create)
        .verifyErrorMessage("Could not find server: test-name");
  }

  @DisplayName("creates client")
  @Test
  void create() {
    assertThat(LocalClientTransport.create("test-name")).isNotNull();
  }

  @DisplayName("throws NullPointerException with null name")
  @Test
  void createNullName() {
    assertThatNullPointerException()
        .isThrownBy(() -> LocalClientTransport.create(null))
        .withMessage("name must not be null");
  }
}
