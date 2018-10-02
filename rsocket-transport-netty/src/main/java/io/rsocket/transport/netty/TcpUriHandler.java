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

package io.rsocket.transport.netty;

import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.uri.UriHandler;
import java.net.URI;
import java.util.Objects;
import java.util.Optional;
import reactor.netty.tcp.TcpServer;

/**
 * An implementation of {@link UriHandler} that creates {@link TcpClientTransport}s and {@link
 * TcpServerTransport}s.
 */
public final class TcpUriHandler implements UriHandler {

  private static final String SCHEME = "tcp";

  @Override
  public Optional<ClientTransport> buildClient(URI uri) {
    Objects.requireNonNull(uri, "uri must not be null");

    if (!SCHEME.equals(uri.getScheme())) {
      return Optional.empty();
    }

    return Optional.of(TcpClientTransport.create(uri.getHost(), uri.getPort()));
  }

  @Override
  public Optional<ServerTransport> buildServer(URI uri) {
    Objects.requireNonNull(uri, "uri must not be null");

    if (!SCHEME.equals(uri.getScheme())) {
      return Optional.empty();
    }

    return Optional.of(TcpServerTransport.create(
        TcpServer.create()
          .host(uri.getHost())
          .port(uri.getPort())));
  }
}
