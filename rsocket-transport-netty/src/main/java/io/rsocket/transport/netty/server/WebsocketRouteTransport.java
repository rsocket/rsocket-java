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
import io.netty.handler.codec.http.HttpMethod;
import io.rsocket.Closeable;
import io.rsocket.DuplexConnection;
import io.rsocket.fragmentation.FragmentationDuplexConnection;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.WebsocketDuplexConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
public final class WebsocketRouteTransport extends BaseWebsocketServerTransport<Closeable> {

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
    this.server = serverConfigurer.apply(Objects.requireNonNull(server, "server must not be null"));
    this.routesBuilder = Objects.requireNonNull(routesBuilder, "routesBuilder must not be null");
    this.template = new UriPathTemplate(Objects.requireNonNull(path, "path must not be null"));
  }

  @Override
  public Mono<Closeable> start(ConnectionAcceptor acceptor, int mtu) {
    Objects.requireNonNull(acceptor, "acceptor must not be null");

    return server
        .route(
            routes -> {
              routesBuilder.accept(routes);
              routes.ws(
                  hsr -> hsr.method().equals(HttpMethod.GET) && template.matches(hsr.uri()),
                  newHandler(acceptor, mtu),
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
      DuplexConnection connection = new WebsocketDuplexConnection((Connection) in);
      if (mtu > 0) {
        connection =
            new FragmentationDuplexConnection(
                connection, ByteBufAllocator.DEFAULT, mtu, false, "server");
      }
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
    private final HashMap<String, Matcher> matchers = new HashMap<>();
    private final HashMap<String, Map<String, String>> vars = new HashMap<>();

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

    /**
     * Matches the template against the given {@code uri} returning a map of path parameters
     * extracted from the uri, keyed by the names in the template. If the uri does not match, or
     * there are no path parameters, an empty map is returned.
     *
     * @param uri The uri to match
     * @return the path parameters from the uri. Never {@code null}.
     */
    final Map<String, String> match(String uri) {
      Map<String, String> pathParameters = vars.get(uri);
      if (null != pathParameters) {
        return pathParameters;
      }

      pathParameters = new HashMap<>();
      Matcher m = matcher(uri);
      if (m.matches()) {
        int i = 1;
        for (String name : pathVariables) {
          String val = m.group(i++);
          pathParameters.put(name, val);
        }
      }
      synchronized (vars) {
        vars.put(uri, pathParameters);
      }

      return pathParameters;
    }

    private Matcher matcher(String uri) {
      uri = filterQueryParams(uri);
      Matcher m = matchers.get(uri);
      if (null == m) {
        m = uriPattern.matcher(uri);
        synchronized (matchers) {
          matchers.put(uri, m);
        }
      }
      return m;
    }
  }
}
