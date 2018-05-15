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

import io.rsocket.Closeable;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.WebsocketDuplexConnection;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.http.server.HttpServer;
import reactor.ipc.netty.http.server.HttpServerRoutes;
import reactor.ipc.netty.http.websocket.WebsocketInbound;
import reactor.ipc.netty.http.websocket.WebsocketOutbound;

public class WebsocketRouteTransport implements ServerTransport<Closeable> {
  private HttpServer server;
  private Consumer<? super HttpServerRoutes> routesBuilder;
  private String path;

  public WebsocketRouteTransport(
      HttpServer server, Consumer<? super HttpServerRoutes> routesBuilder, String path) {
    this.server = server;
    this.routesBuilder = routesBuilder;
    this.path = path;
  }

  @Override
  public Mono<Closeable> start(ConnectionAcceptor acceptor) {
    return server
        .newRouter(
            routes -> {
              routesBuilder.accept(routes);
              routes.ws(path, newHandler(acceptor));
            })
        .map(NettyContextCloseable::new);
  }

  public static BiFunction<WebsocketInbound, WebsocketOutbound, Publisher<Void>> newHandler(
      ConnectionAcceptor acceptor) {
    return (in, out) -> {
      WebsocketDuplexConnection connection = new WebsocketDuplexConnection(in, out, in.context());
      acceptor.apply(connection).subscribe();

      return out.neverComplete();
    };
  }
}
