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

package io.rsocket.transport.netty.server;

import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.ArgumentMatchers.any;

import java.net.InetSocketAddress;
import java.util.function.BiFunction;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;
import reactor.netty.http.server.WebsocketServerSpec;
import reactor.test.StepVerifier;

final class WebsocketServerTransportTest {

  @Test
  public void testThatSetupWithUnSpecifiedFrameSizeShouldSetMaxFrameSize() {
    ArgumentCaptor<BiFunction> httpHandlerCaptor = ArgumentCaptor.forClass(BiFunction.class);
    HttpServer server = Mockito.spy(HttpServer.create());
    Mockito.doAnswer(a -> server).when(server).handle(httpHandlerCaptor.capture());
    Mockito.doAnswer(a -> server).when(server).tcpConfiguration(any());
    Mockito.doAnswer(a -> Mono.empty()).when(server).bind();

    WebsocketServerTransport serverTransport = WebsocketServerTransport.create(server);
    serverTransport.start(c -> Mono.empty()).subscribe();

    HttpServerRequest httpServerRequest = Mockito.mock(HttpServerRequest.class);
    HttpServerResponse httpServerResponse = Mockito.mock(HttpServerResponse.class);

    httpHandlerCaptor.getValue().apply(httpServerRequest, httpServerResponse);

    ArgumentCaptor<BiFunction> handlerCaptor = ArgumentCaptor.forClass(BiFunction.class);
    ArgumentCaptor<WebsocketServerSpec> specCaptor =
        ArgumentCaptor.forClass(WebsocketServerSpec.class);

    Mockito.verify(httpServerResponse).sendWebsocket(handlerCaptor.capture(), specCaptor.capture());

    WebsocketServerSpec spec = specCaptor.getValue();
    assertThat(spec.maxFramePayloadLength()).isEqualTo(FRAME_LENGTH_MASK);
  }

  @DisplayName("creates server with BindAddress")
  @Test
  void createBindAddress() {
    assertThat(WebsocketServerTransport.create("test-bind-address", 8000)).isNotNull();
  }

  @DisplayName("creates server with HttpClient")
  @Test
  void createHttpClient() {
    assertThat(WebsocketServerTransport.create(HttpServer.create())).isNotNull();
  }

  @DisplayName("creates server with InetSocketAddress")
  @Test
  void createInetSocketAddress() {
    assertThat(
            WebsocketServerTransport.create(
                InetSocketAddress.createUnresolved("test-bind-address", 8000)))
        .isNotNull();
  }

  @DisplayName("create throws NullPointerException with null bindAddress")
  @Test
  void createNullBindAddress() {
    assertThatNullPointerException()
        .isThrownBy(() -> WebsocketServerTransport.create(null, 8000))
        .withMessage("bindAddress must not be null");
  }

  @DisplayName("create throws NullPointerException with null client")
  @Test
  void createNullHttpClient() {
    assertThatNullPointerException()
        .isThrownBy(() -> WebsocketServerTransport.create((HttpServer) null))
        .withMessage("server must not be null");
  }

  @DisplayName("create throws NullPointerException with null address")
  @Test
  void createNullInetSocketAddress() {
    assertThatNullPointerException()
        .isThrownBy(() -> WebsocketServerTransport.create((InetSocketAddress) null))
        .withMessage("address must not be null");
  }

  @DisplayName("creates server with port")
  @Test
  void createPort() {
    assertThat(WebsocketServerTransport.create(8000)).isNotNull();
  }

  @DisplayName("starts server")
  @Test
  void start() {
    InetSocketAddress address = InetSocketAddress.createUnresolved("localhost", 0);

    WebsocketServerTransport serverTransport = WebsocketServerTransport.create(address);

    serverTransport
        .start(duplexConnection -> Mono.empty())
        .as(StepVerifier::create)
        .expectNextCount(1)
        .verifyComplete();
  }

  @DisplayName("start throws NullPointerException with null acceptor")
  @Test
  void startNullAcceptor() {
    assertThatNullPointerException()
        .isThrownBy(() -> WebsocketServerTransport.create(8000).start(null))
        .withMessage("acceptor must not be null");
  }
}
