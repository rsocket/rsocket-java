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

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.WebsocketDuplexConnection;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Objects;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.http.server.HttpServer;

/**
 * An implementation of {@link ServerTransport} that connects to a {@link ClientTransport} via a
 * Websocket.
 */
public final class WebsocketServerTransport
    extends BaseWebsocketServerTransport<WebsocketServerTransport, CloseableChannel> {

  private final HttpServer server;

  private HttpHeaders headers = new DefaultHttpHeaders();

  private WebsocketServerTransport(HttpServer server) {
    this.server = serverConfigurer.apply(Objects.requireNonNull(server, "server must not be null"));
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
   * Add a header and value(s) to set on the response of WebSocket handshakes.
   *
   * @param name the header name
   * @param values the header value(s)
   * @return the same instance for method chaining
   * @since 1.0.1
   */
  public WebsocketServerTransport header(String name, String... values) {
    if (values != null) {
      Arrays.stream(values).forEach(value -> headers.add(name, value));
    }
    return this;
  }

  @Override
  public Mono<CloseableChannel> start(ConnectionAcceptor acceptor) {
    Objects.requireNonNull(acceptor, "acceptor must not be null");
    return server
        .handle(
            (request, response) -> {
              response.headers(headers);
              return response.sendWebsocket(
                  (in, out) ->
                      acceptor
                          .apply(new WebsocketDuplexConnection("server", (Connection) in))
                          .then(out.neverComplete()),
                  specBuilder.build());
            })
        .bind()
        .map(CloseableChannel::new);
  }
}
