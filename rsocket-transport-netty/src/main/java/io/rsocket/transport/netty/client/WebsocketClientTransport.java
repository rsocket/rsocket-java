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

package io.rsocket.transport.netty.client;

import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.rsocket.DuplexConnection;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.WebsocketDuplexConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Consumer;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.WebsocketClientSpec;
import reactor.netty.tcp.TcpClient;

/**
 * An implementation of {@link ClientTransport} that connects to a {@link ServerTransport} over
 * WebSocket.
 */
public final class WebsocketClientTransport implements ClientTransport {

  private static final String DEFAULT_PATH = "/";

  private final HttpClient client;

  private final String path;

  private HttpHeaders headers = new DefaultHttpHeaders();

  private final WebsocketClientSpec.Builder specBuilder =
      WebsocketClientSpec.builder().maxFramePayloadLength(FRAME_LENGTH_MASK);

  private WebsocketClientTransport(HttpClient client, String path) {
    Objects.requireNonNull(client, "HttpClient must not be null");
    Objects.requireNonNull(path, "path must not be null");
    this.client = client;
    this.path = path.startsWith("/") ? path : "/" + path;
  }

  /**
   * Creates a new instance connecting to localhost
   *
   * @param port the port to connect to
   * @return a new instance
   */
  public static WebsocketClientTransport create(int port) {
    return create(TcpClient.create().port(port));
  }

  /**
   * Creates a new instance
   *
   * @param bindAddress the address to connect to
   * @param port the port to connect to
   * @return a new instance
   * @throws NullPointerException if {@code bindAddress} is {@code null}
   */
  public static WebsocketClientTransport create(String bindAddress, int port) {
    return create(TcpClient.create().host(bindAddress).port(port));
  }

  /**
   * Creates a new instance
   *
   * @param address the address to connect to
   * @return a new instance
   * @throws NullPointerException if {@code address} is {@code null}
   */
  public static WebsocketClientTransport create(InetSocketAddress address) {
    Objects.requireNonNull(address, "address must not be null");
    return create(TcpClient.create().remoteAddress(() -> address));
  }

  /**
   * Creates a new instance
   *
   * @param client the {@link TcpClient} to use
   * @return a new instance
   * @throws NullPointerException if {@code client} or {@code path} is {@code null}
   */
  public static WebsocketClientTransport create(TcpClient client) {
    return new WebsocketClientTransport(HttpClient.from(client), DEFAULT_PATH);
  }

  /**
   * Creates a new instance
   *
   * @param uri the URI to connect to
   * @return a new instance
   * @throws NullPointerException if {@code uri} is {@code null}
   */
  public static WebsocketClientTransport create(URI uri) {
    Objects.requireNonNull(uri, "uri must not be null");
    boolean isSecure = uri.getScheme().equals("wss") || uri.getScheme().equals("https");
    TcpClient client =
        (isSecure ? TcpClient.create().secure() : TcpClient.create())
            .host(uri.getHost())
            .port(uri.getPort() == -1 ? (isSecure ? 443 : 80) : uri.getPort());
    return new WebsocketClientTransport(HttpClient.from(client), uri.getPath());
  }

  /**
   * Creates a new instance
   *
   * @param client the {@link HttpClient} to use
   * @param path the path to request
   * @return a new instance
   * @throws NullPointerException if {@code client} or {@code path} is {@code null}
   */
  public static WebsocketClientTransport create(HttpClient client, String path) {
    return new WebsocketClientTransport(client, path);
  }

  /**
   * Add a header and value(s) to use for the WebSocket handshake request.
   *
   * @param name the header name
   * @param values the header value(s)
   * @return the same instance for method chaining
   * @since 1.0.1
   */
  public WebsocketClientTransport header(String name, String... values) {
    if (values != null) {
      Arrays.stream(values).forEach(value -> headers.add(name, value));
    }
    return this;
  }

  /**
   * Provide a consumer to customize properties of the {@link WebsocketClientSpec} to use for
   * WebSocket upgrades. The consumer is invoked immediately.
   *
   * @param configurer the configurer to apply to the spec
   * @return the same instance for method chaining
   * @since 1.0.1
   */
  public WebsocketClientTransport webSocketSpec(Consumer<WebsocketClientSpec.Builder> configurer) {
    configurer.accept(specBuilder);
    return this;
  }

  @Override
  public int maxFrameLength() {
    return specBuilder.build().maxFramePayloadLength();
  }

  @Override
  public Mono<DuplexConnection> connect() {
    return client
        .headers(headers -> headers.add(this.headers))
        .websocket(specBuilder.build())
        .uri(path)
        .connect()
        .map(connection -> new WebsocketDuplexConnection("client", connection));
  }
}
