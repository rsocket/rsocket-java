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

package io.rsocket.transport.netty;

import static org.assertj.core.api.Assertions.assertThat;

import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.transport.netty.server.WebsocketServerTransport;
import io.rsocket.uri.UriTransportRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

final class WebsocketUriTransportRegistryTest {

  @DisplayName("non-ws URI does not return WebsocketClientTransport")
  @Test
  void clientForUriInvalid() {
    assertThat(UriTransportRegistry.clientForUri("amqp://localhost"))
        .isNotInstanceOf(TcpClientTransport.class)
        .isNotInstanceOf(WebsocketClientTransport.class);
  }

  @DisplayName("ws URI returns WebsocketClientTransport")
  @Test
  void clientForUriWebsocket() {
    assertThat(UriTransportRegistry.clientForUri("ws://test:9898"))
        .isInstanceOf(WebsocketClientTransport.class);
  }

  @DisplayName("non-ws URI does not return WebsocketServerTransport")
  @Test
  void serverForUriInvalid() {
    assertThat(UriTransportRegistry.serverForUri("amqp://localhost"))
        .isNotInstanceOf(TcpServerTransport.class)
        .isNotInstanceOf(WebsocketServerTransport.class);
  }

  @DisplayName("ws URI returns WebsocketServerTransport")
  @Test
  void serverForUriWebsocket() {
    assertThat(UriTransportRegistry.serverForUri("ws://test:9898"))
        .isInstanceOf(WebsocketServerTransport.class);
  }
}
