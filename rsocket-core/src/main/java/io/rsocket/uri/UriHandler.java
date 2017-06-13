package io.rsocket.uri;

import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import java.net.URI;
import java.util.Optional;
import java.util.ServiceLoader;

/**
 * URI to {@link ClientTransport} or {@link ServerTransport}. Should return a non empty value only
 * when the URI is unambiguously mapped to a particular transport, either by a standardised
 * implementation or via some flag in the URI to indicate a choice.
 */
public interface UriHandler {
  static ServiceLoader<UriHandler> loadServices() {
    return ServiceLoader.load(UriHandler.class);
  }

  default Optional<ClientTransport> buildClient(URI uri) {
    return Optional.empty();
  }

  default Optional<ServerTransport> buildServer(URI uri) {
    return Optional.empty();
  }
}
