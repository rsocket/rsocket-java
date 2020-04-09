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

import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Closeable;
import io.rsocket.DuplexConnection;
import io.rsocket.fragmentation.FragmentationDuplexConnection;
import io.rsocket.fragmentation.ReassemblyDuplexConnection;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.WebsocketDuplexConnection;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.server.HttpServerRoutes;
import reactor.netty.http.server.WebsocketServerSpec;
import reactor.netty.http.websocket.WebsocketInbound;
import reactor.netty.http.websocket.WebsocketOutbound;

/**
 * An implementation of {@link ServerTransport} that connects via Websocket and listens on specified
 * routes.
 */
public final class WebsocketRouteTransport extends BaseWebsocketServerTransport<Closeable> {

  private final String path;

  private final Consumer<? super HttpServerRoutes> routesBuilder;

  private final HttpServer server;

  /**
   * Creates a new instance
   *
   * @param server the {@link HttpServer} to use
   * @param routesBuilder the builder for the routes that will be listened on
   * @param path the path foe each route
   */
  public WebsocketRouteTransport(
      HttpServer server, Consumer<? super HttpServerRoutes> routesBuilder, String path) {
    this.server = serverConfigurer.apply(Objects.requireNonNull(server, "server must not be null"));
    this.routesBuilder = Objects.requireNonNull(routesBuilder, "routesBuilder must not be null");
    this.path = Objects.requireNonNull(path, "path must not be null");
  }

  @Override
  public Mono<Closeable> start(ConnectionAcceptor acceptor, int mtu) {
    Objects.requireNonNull(acceptor, "acceptor must not be null");

    return server
        .route(
            routes -> {
              routesBuilder.accept(routes);
              routes.ws(
                  path,
                  newHandler(acceptor, mtu),
                  WebsocketServerSpec.builder().maxFramePayloadLength(FRAME_LENGTH_MASK).build());
            })
        .bind()
        .map(CloseableChannel::new);
  }

  /**
   * Creates a new Websocket handler
   *
   * @param acceptor the {@link ConnectionAcceptor} to use with the handler
   * @return a new Websocket handler
   * @throws NullPointerException if {@code acceptor} is {@code null}
   */
  public static BiFunction<WebsocketInbound, WebsocketOutbound, Publisher<Void>> newHandler(
      ConnectionAcceptor acceptor) {
    return newHandler(acceptor, 0);
  }

  /**
   * Creates a new Websocket handler
   *
   * @param acceptor the {@link ConnectionAcceptor} to use with the handler
   * @param mtu the fragment size
   * @return a new Websocket handler
   * @throws NullPointerException if {@code acceptor} is {@code null}
   */
  public static BiFunction<WebsocketInbound, WebsocketOutbound, Publisher<Void>> newHandler(
      ConnectionAcceptor acceptor, int mtu) {
    return (in, out) -> {
      DuplexConnection connection =
          new ReassemblyDuplexConnection(
              new WebsocketDuplexConnection((Connection) in), ByteBufAllocator.DEFAULT, false);
      if (mtu > 0) {
        connection =
            new FragmentationDuplexConnection(
                connection, ByteBufAllocator.DEFAULT, mtu, false, "server");
      }
      return acceptor.apply(connection).then(out.neverComplete());
    };
  }
}
