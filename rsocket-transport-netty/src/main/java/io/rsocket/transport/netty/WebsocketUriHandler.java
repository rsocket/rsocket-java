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
