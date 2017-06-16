/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.transport.netty.client;

import io.rsocket.DuplexConnection;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.netty.WebsocketDuplexConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.http.client.HttpClient;

public class WebsocketClientTransport implements ClientTransport {
  private final HttpClient client;
  private String path;

  private WebsocketClientTransport(HttpClient client, String path) {
    this.client = client;
    this.path = path;
  }

  public static WebsocketClientTransport create(int port) {
    HttpClient httpClient = HttpClient.create(port);
    return create(httpClient, "/");
  }

  public static WebsocketClientTransport create(String bindAddress, int port) {
    HttpClient httpClient = HttpClient.create(bindAddress, port);
    return create(httpClient, "/");
  }

  public static WebsocketClientTransport create(InetSocketAddress address) {
    return create(address.getHostName(), address.getPort());
  }

  public static WebsocketClientTransport create(URI uri) {
    HttpClient httpClient = createClient(uri);
    return create(httpClient, uri.getPath());
  }

  private static HttpClient createClient(URI uri) {
    if (isSecureWebsocket(uri)) {
      return HttpClient.create(
          options -> options.sslSupport().connect(uri.getHost(), getPort(uri, 443)));
    } else {
      return HttpClient.create(uri.getHost(), getPort(uri, 80));
    }
  }

  public static int getPort(URI uri, int defaultPort) {
    return uri.getPort() == -1 ? defaultPort : uri.getPort();
  }

  public static boolean isSecureWebsocket(URI uri) {
    return uri.getScheme().equals("wss") || uri.getScheme().equals("https");
  }

  public static boolean isPlaintextWebsocket(URI uri) {
    return uri.getScheme().equals("ws") || uri.getScheme().equals("http");
  }

  public static WebsocketClientTransport create(HttpClient client, String path) {
    return new WebsocketClientTransport(client, path);
  }

  @Override
  public Mono<DuplexConnection> connect() {
    return Mono.create(
        sink ->
            client
                .ws(path)
                .then(
                    response ->
                        response.receiveWebsocket(
                            (in, out) -> {
                              WebsocketDuplexConnection connection =
                                  new WebsocketDuplexConnection(in, out, in.context());
                              sink.success(connection);
                              return connection.onClose();
                            }))
                .doOnError(sink::error)
                .subscribe());
  }
}
