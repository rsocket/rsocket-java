package io.rsocket.transport.local;

import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.uri.UriHandler;
import java.net.URI;
import java.util.Optional;

public class LocalUriHandler implements UriHandler {
  @Override
  public Optional<ClientTransport> buildClient(URI uri) {
    if (uri.getScheme().equals("local")) {
      return Optional.of(LocalClientTransport.create(uri.getSchemeSpecificPart()));
    }

    return UriHandler.super.buildClient(uri);
  }

  @Override
  public Optional<ServerTransport> buildServer(URI uri) {
    if (uri.getScheme().equals("local")) {
      return Optional.of(LocalServerTransport.create(uri.getSchemeSpecificPart()));
    }

    return UriHandler.super.buildServer(uri);
  }
}
