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

package io.rsocket.transport.netty.client;

import static io.rsocket.transport.netty.UriUtils.getPort;
import static io.rsocket.transport.netty.UriUtils.isSecure;

import io.rsocket.DuplexConnection;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.TransportHeaderAware;
import io.rsocket.transport.netty.WebsocketDuplexConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.http.client.HttpClient;
import reactor.netty.tcp.TcpClient;

/**
 * An implementation of {@link ClientTransport} that connects to a {@link ServerTransport} via a
 * Websocket.
 */
public final class WebsocketClientTransport implements ClientTransport, TransportHeaderAware {

  private final HttpClient client;

  private String path;

  private Supplier<Map<String, String>> transportHeaders = Collections::emptyMap;

  private WebsocketClientTransport(HttpClient client, String path) {
    this.client = client;
    this.path = path;
  }

  /**
   * Creates a new instance connecting to localhost
   *
   * @param port the port to connect to
   * @return a new instance
   */
  public static WebsocketClientTransport create(int port) {
    TcpClient client = TcpClient.create().port(port);
    return create(client);
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
    Objects.requireNonNull(bindAddress, "bindAddress must not be null");

    TcpClient client = TcpClient.create().host(bindAddress).port(port);
    return create(client);
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

    TcpClient client = TcpClient.create().addressSupplier(() -> address);
    return create(client);
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

    TcpClient client = createClient(uri);
    return create(HttpClient.from(client), uri.getPath());
  }

  /**
   * Creates a new instance
   *
   * @param client the {@link TcpClient} to use
   * @return a new instance
   * @throws NullPointerException if {@code client} or {@code path} is {@code null}
   */
  public static WebsocketClientTransport create(TcpClient client) {
    Objects.requireNonNull(client, "client must not be null");

    return create(HttpClient.from(client), "/");
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
    Objects.requireNonNull(client, "client must not be null");
    Objects.requireNonNull(path, "path must not be null");

    return new WebsocketClientTransport(client, path);
  }

  @Override
  public Mono<DuplexConnection> connect() {
      return client
          .headers(headers -> transportHeaders.get().forEach(headers::set))
          .websocket()
          .uri(path)
          .connect()
          .map(WebsocketDuplexConnection::new);
  }

  @Override
  public void setTransportHeaders(Supplier<Map<String, String>> transportHeaders) {
    this.transportHeaders =
        Objects.requireNonNull(transportHeaders, "transportHeaders must not be null");
  }

  private static TcpClient createClient(URI uri) {
    if (isSecure(uri)) {
      return TcpClient.create()
          .secure()
          .host(uri.getHost())
          .port(getPort(uri, 443));
    } else {
      return TcpClient.create()
          .host(uri.getHost())
          .port(getPort(uri, 80));
    }
  }
}
