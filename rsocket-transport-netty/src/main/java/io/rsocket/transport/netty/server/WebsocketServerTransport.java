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

import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;

import io.rsocket.fragmentation.FragmentationDuplexConnection;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.TransportHeaderAware;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;
import reactor.netty.http.server.WebsocketServerSpec;
import reactor.netty.http.websocket.WebsocketInbound;
import reactor.netty.http.websocket.WebsocketOutbound;

/**
 * An implementation of {@link ServerTransport} that connects to a {@link ClientTransport} via a
 * Websocket.
 */
public final class WebsocketServerTransport extends BaseWebsocketServerTransport<CloseableChannel>
    implements TransportHeaderAware {
  private static final Logger logger = LoggerFactory.getLogger(WebsocketServerTransport.class);

  final HttpServer server;
  final Handler handler;

  private Supplier<Map<String, String>> transportHeaders = Collections::emptyMap;

  private WebsocketServerTransport(HttpServer server) {
    this.server = serverConfigurer.apply(Objects.requireNonNull(server, "server must not be null"));
    this.handler =
        (request, response, webSocketHandler) -> {
          transportHeaders.get().forEach(response::addHeader);
          return response.sendWebsocket(
              webSocketHandler,
              WebsocketServerSpec.builder().maxFramePayloadLength(FRAME_LENGTH_MASK).build());
        };
  }

  private WebsocketServerTransport(HttpServer server, Handler handler) {
    this.server = serverConfigurer.apply(Objects.requireNonNull(server, "server must not be null"));
    this.handler = Objects.requireNonNull(handler, "handler must not be null");
  }

  /**
   * Creates a new instance binding to localhost
   *
   * @param port the port to bind to
   * @return a new instance
   */
  public static WebsocketServerTransport create(int port) {
    HttpServer httpServer = HttpServer.create().port(port);
    return create(httpServer);
  }

  /**
   * Creates a new instance
   *
   * @param bindAddress the address to bind to
   * @param port the port to bind to
   * @return a new instance
   * @throws NullPointerException if {@code bindAddress} is {@code null}
   */
  public static WebsocketServerTransport create(String bindAddress, int port) {
    Objects.requireNonNull(bindAddress, "bindAddress must not be null");

    HttpServer httpServer = HttpServer.create().host(bindAddress).port(port);
    return create(httpServer);
  }

  /**
   * Creates a new instance
   *
   * @param address the address to bind to
   * @return a new instance
   * @throws NullPointerException if {@code address} is {@code null}
   */
  public static WebsocketServerTransport create(InetSocketAddress address) {
    Objects.requireNonNull(address, "address must not be null");

    return create(address.getHostName(), address.getPort());
  }

  /**
   * Creates a new instance
   *
   * @param server the {@link HttpServer} to use
   * @return a new instance
   * @throws NullPointerException if {@code server} is {@code null}
   */
  public static WebsocketServerTransport create(final HttpServer server) {
    Objects.requireNonNull(server, "server must not be null");

    return new WebsocketServerTransport(server);
  }

  /**
   * Creates a new instance
   *
   * @param server the {@link HttpServer} to use
   * @param handler websocket handling chain customizer.
   *  Should be used in order to customize / reject response from the
   *  {@link WebsocketServerTransport}.
   *  Can be used as shown in the following sample:
   *
   *  <pre>
   *      {@code
   *  new Handler() {
   *    @Override
   *    public Publisher<Void> handle(HttpServerRequest request,
   *            HttpServerResponse response,
   *            BiFunction<WebsocketInbound, WebsocketOutbound, Publisher<Void>> webSocketHandler) {
   *
   *      return response.sendWebsocket(webSocketHandler,
   *              WebsocketServerSpec.builder()
   *                                 .maxFramePayloadLength(FRAME_LENGTH_MASK)
   *                                 .build());
   *    }
   *  }
   *  </pre>
   * @return a new instance
   * @throws NullPointerException if {@code server} is {@code null}
   */
  public static WebsocketServerTransport create(final HttpServer server, final Handler handler) {
    Objects.requireNonNull(server, "server must not be null");
    Objects.requireNonNull(handler, "handler must not be null");

    return new WebsocketServerTransport(server, handler);
  }

  /**
   * @deprecated in favor of {@link #create(HttpServer, Handler)} which might be used as the following
   * in order
   *     to provide custom headers in the response
   * <pre>
   *      {@code
   *  new Handler() {
   *    @Override
   *    public Publisher<Void> handle(HttpServerRequest request,
   *            HttpServerResponse response,
   *            BiFunction<WebsocketInbound, WebsocketOutbound, Publisher<Void>> webSocketHandler) {
   *      response.addHeader("test", "test");
   *      return response.sendWebsocket(webSocketHandler,
   *              WebsocketServerSpec.builder()
   *                                 .maxFramePayloadLength(FRAME_LENGTH_MASK)
   *                                 .build());
   *    }
   *  }
   *  </pre>
   */
  @Deprecated
  @Override
  public void setTransportHeaders(Supplier<Map<String, String>> transportHeaders) {
    this.transportHeaders =
        Objects.requireNonNull(transportHeaders, "transportHeaders must not be null");
  }

  @Override
  public Mono<CloseableChannel> start(ConnectionAcceptor acceptor, int mtu) {
    Objects.requireNonNull(acceptor, "acceptor must not be null");

    Mono<CloseableChannel> isError = FragmentationDuplexConnection.checkMtu(mtu);
    return isError != null
        ? isError
        : server
            .handle(
                (request, response) -> handler.handle(request, response, newHandler(acceptor, mtu)))
            .bind()
            .map(CloseableChannel::new);
  }

  /**
   * Websocket handling chain customizer.
   * Should be used in order to customize / reject response from the
   * {@link WebsocketServerTransport}.
   * Can be used as shown in the following sample:
   *
   * <pre>
   *     {@code
   * new Handler() {
   *   @Override
   *   public Publisher<Void> handle(HttpServerRequest request,
   *           HttpServerResponse response,
   *           BiFunction<WebsocketInbound, WebsocketOutbound, Publisher<Void>> webSocketHandler) {
   *
   *     return response.sendWebsocket(webSocketHandler,
   *             WebsocketServerSpec.builder()
   *                                .maxFramePayloadLength(FRAME_LENGTH_MASK)
   *                                .build());
   *   }
   * }
   * </pre>
   */
  public interface Handler {

    Publisher<Void> handle(
        HttpServerRequest request,
        HttpServerResponse response,
        BiFunction<WebsocketInbound, WebsocketOutbound, Publisher<Void>> webSocketHandler);
  }
}
