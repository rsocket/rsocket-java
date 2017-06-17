/*
 * Copyright 2016 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.rsocket.transport.netty;

import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.rsocket.transport.netty.server.WebsocketServerTransport;
import io.rsocket.uri.UriHandler;
import java.net.URI;
import java.util.Optional;

public class WebsocketUriHandler implements UriHandler {
  @Override
  public Optional<ClientTransport> buildClient(URI uri) {
    if (uri.getScheme().equals("ws") || uri.getScheme().equals("wss")) {
      return Optional.of(WebsocketClientTransport.create(uri));
    }

    return UriHandler.super.buildClient(uri);
  }

  @Override
  public Optional<ServerTransport> buildServer(URI uri) {
    if (uri.getScheme().equals("ws")) {
      return Optional.of(
          WebsocketServerTransport.create(
              uri.getHost(), WebsocketClientTransport.getPort(uri, 80)));
    }

    return UriHandler.super.buildServer(uri);
  }
}
