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

import static io.rsocket.frame.FrameHeaderFlyweight.FRAME_LENGTH_MASK;

import io.netty.handler.codec.http.HttpMethod;
import io.rsocket.Closeable;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.WebsocketDuplexConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.server.HttpServerRoutes;
import reactor.netty.http.websocket.WebsocketInbound;
import reactor.netty.http.websocket.WebsocketOutbound;

/**
 * An implementation of {@link ServerTransport} that connects via Websocket and listens on specified
 * routes.
 */
public final class WebsocketRouteTransport implements ServerTransport<Closeable> {

  private final UriPathTemplate template;

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

    this.server = Objects.requireNonNull(server, "server must not be null");
    this.routesBuilder = Objects.requireNonNull(routesBuilder, "routesBuilder must not be null");
    this.template = new UriPathTemplate(Objects.requireNonNull(path, "path must not be null"));
  }

  @Override
  public Mono<Closeable> start(ConnectionAcceptor acceptor) {
    Objects.requireNonNull(acceptor, "acceptor must not be null");

    return server
        .route(
            routes -> {
              routesBuilder.accept(routes);
              routes.ws(
                  hsr -> hsr.method().equals(HttpMethod.GET) && template.matches(hsr.uri()),
                  newHandler(acceptor),
                  null,
                  FRAME_LENGTH_MASK);
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

    Objects.requireNonNull(acceptor, "acceptor must not be null");

    return (in, out) -> {
      WebsocketDuplexConnection connection = new WebsocketDuplexConnection((Connection) in);
      return acceptor.apply(connection).then(out.neverComplete());
    };
  }

  static final class UriPathTemplate {

    private static final Pattern FULL_SPLAT_PATTERN = Pattern.compile("[\\*][\\*]");
    private static final String FULL_SPLAT_REPLACEMENT = ".*";

    private static final Pattern NAME_SPLAT_PATTERN = Pattern.compile("\\{([^/]+?)\\}[\\*][\\*]");
    private static final String NAME_SPLAT_REPLACEMENT = "(?<%NAME%>.*)";

    private static final Pattern NAME_PATTERN = Pattern.compile("\\{([^/]+?)\\}");
    private static final String NAME_REPLACEMENT = "(?<%NAME%>[^\\/]*)";

    private final List<String> pathVariables = new ArrayList<>();
    private final Map<String, Matcher> matchers = new ConcurrentHashMap<>();

    private final Pattern uriPattern;

    static String filterQueryParams(String uri) {
      int hasQuery = uri.lastIndexOf("?");
      if (hasQuery != -1) {
        return uri.substring(0, hasQuery);
      } else {
        return uri;
      }
    }

    /**
     * Creates a new {@code UriPathTemplate} from the given {@code uriPattern}.
     *
     * @param uriPattern The pattern to be used by the template
     */
    UriPathTemplate(String uriPattern) {
      String s = "^" + filterQueryParams(uriPattern);

      Matcher m = NAME_SPLAT_PATTERN.matcher(s);
      while (m.find()) {
        for (int i = 1; i <= m.groupCount(); i++) {
          String name = m.group(i);
          pathVariables.add(name);
          s = m.replaceFirst(NAME_SPLAT_REPLACEMENT.replaceAll("%NAME%", name));
          m.reset(s);
        }
      }

      m = NAME_PATTERN.matcher(s);
      while (m.find()) {
        for (int i = 1; i <= m.groupCount(); i++) {
          String name = m.group(i);
          pathVariables.add(name);
          s = m.replaceFirst(NAME_REPLACEMENT.replaceAll("%NAME%", name));
          m.reset(s);
        }
      }

      m = FULL_SPLAT_PATTERN.matcher(s);
      while (m.find()) {
        s = m.replaceAll(FULL_SPLAT_REPLACEMENT);
        m.reset(s);
      }

      this.uriPattern = Pattern.compile(s + "$");
    }

    /**
     * Tests the given {@code uri} against this template, returning {@code true} if the uri matches
     * the template, {@code false} otherwise.
     *
     * @param uri The uri to match
     * @return {@code true} if there's a match, {@code false} otherwise
     */
    public boolean matches(String uri) {
      return matcher(uri).matches();
    }

    private Matcher matcher(String uri) {
      final String foundUri = filterQueryParams(uri);
      return matchers.computeIfAbsent(uri, __ -> uriPattern.matcher(foundUri));
    }
  }
}
