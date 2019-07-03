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

import static io.rsocket.frame.FrameLengthFlyweight.FRAME_LENGTH_MASK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.function.BiFunction;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;
import reactor.test.StepVerifier;

final class WebsocketServerTransportTest {

  // @Test
  public void testThatSetupWithUnSpecifiedFrameSizeShouldSetMaxFrameSize() {
    ArgumentCaptor<BiFunction> captor = ArgumentCaptor.forClass(BiFunction.class);
    HttpServer httpServer = Mockito.spy(HttpServer.create());
    Mockito.doAnswer(a -> httpServer).when(httpServer).handle(captor.capture());
    Mockito.doAnswer(a -> Mono.empty()).when(httpServer).bind();

    WebsocketServerTransport serverTransport = WebsocketServerTransport.create(httpServer);

    serverTransport.start(c -> Mono.empty(), 0).subscribe();

    HttpServerRequest httpServerRequest = Mockito.mock(HttpServerRequest.class);
    HttpServerResponse httpServerResponse = Mockito.mock(HttpServerResponse.class);

    captor.getValue().apply(httpServerRequest, httpServerResponse);

    Mockito.verify(httpServerResponse)
        .sendWebsocket(
            Mockito.nullable(String.class), Mockito.eq(FRAME_LENGTH_MASK), Mockito.any());
  }

  // @Test
  public void testThatSetupWithSpecifiedFrameSizeButLowerThanWsDefaultShouldSetToWsDefault() {
    ArgumentCaptor<BiFunction> captor = ArgumentCaptor.forClass(BiFunction.class);
    HttpServer httpServer = Mockito.spy(HttpServer.create());
    Mockito.doAnswer(a -> httpServer).when(httpServer).handle(captor.capture());
    Mockito.doAnswer(a -> Mono.empty()).when(httpServer).bind();

    WebsocketServerTransport serverTransport = WebsocketServerTransport.create(httpServer);

    serverTransport.start(c -> Mono.empty(), 1000).subscribe();

    HttpServerRequest httpServerRequest = Mockito.mock(HttpServerRequest.class);
    HttpServerResponse httpServerResponse = Mockito.mock(HttpServerResponse.class);

    captor.getValue().apply(httpServerRequest, httpServerResponse);

    Mockito.verify(httpServerResponse)
        .sendWebsocket(
            Mockito.nullable(String.class), Mockito.eq(FRAME_LENGTH_MASK), Mockito.any());
  }

  // @Test
  public void
      testThatSetupWithSpecifiedFrameSizeButHigherThanWsDefaultShouldSetToSpecifiedFrameSize() {
    ArgumentCaptor<BiFunction> captor = ArgumentCaptor.forClass(BiFunction.class);
    HttpServer httpServer = Mockito.spy(HttpServer.create());
    Mockito.doAnswer(a -> httpServer).when(httpServer).handle(captor.capture());
    Mockito.doAnswer(a -> Mono.empty()).when(httpServer).bind();

    WebsocketServerTransport serverTransport = WebsocketServerTransport.create(httpServer);

    serverTransport.start(c -> Mono.empty(), 65536 + 1000).subscribe();

    HttpServerRequest httpServerRequest = Mockito.mock(HttpServerRequest.class);
    HttpServerResponse httpServerResponse = Mockito.mock(HttpServerResponse.class);

    captor.getValue().apply(httpServerRequest, httpServerResponse);

    Mockito.verify(httpServerResponse)
        .sendWebsocket(
            Mockito.nullable(String.class), Mockito.eq(FRAME_LENGTH_MASK), Mockito.any());
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

  @DisplayName("sets transport headers")
  @Test
  void setTransportHeader() {
    WebsocketServerTransport.create(8000).setTransportHeaders(Collections::emptyMap);
  }

  @DisplayName("setTransportHeaders throws NullPointerException with null headers")
  @Test
  void setTransportHeadersNullHeaders() {
    assertThatNullPointerException()
        .isThrownBy(() -> WebsocketServerTransport.create(8000).setTransportHeaders(null))
        .withMessage("transportHeaders must not be null");
  }

  @DisplayName("starts server")
  @Test
  void start() {
    InetSocketAddress address = InetSocketAddress.createUnresolved("localhost", 0);

    WebsocketServerTransport serverTransport = WebsocketServerTransport.create(address);

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
        .isThrownBy(() -> WebsocketServerTransport.create(8000).start(null, 0))
        .withMessage("acceptor must not be null");
  }
}
