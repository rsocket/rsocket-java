package io.rsocket.uri;

import static io.rsocket.uri.UriHandler.loadServices;

import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import reactor.core.publisher.Mono;

/**
 * Registry for looking up transports by URI.
 *
 * <p>Uses the Jar Services mechanism with services defined by {@link UriHandler}.
 */
public class UriTransportRegistry {
  private static final ClientTransport FAILED_CLIENT_LOOKUP =
      () -> Mono.error(new UnsupportedOperationException());
  private static final ServerTransport FAILED_SERVER_LOOKUP =
      acceptor -> Mono.error(new UnsupportedOperationException());

  private List<UriHandler> handlers;

  public UriTransportRegistry(ServiceLoader<UriHandler> services) {
    handlers = new ArrayList<>();
    services.forEach(handlers::add);
  }

  public static UriTransportRegistry fromServices() {
    ServiceLoader<UriHandler> services = loadServices();

    return new UriTransportRegistry(services);
  }

  public static ClientTransport clientForUri(String uri) {
    return UriTransportRegistry.fromServices().findClient(uri);
  }

  private ClientTransport findClient(String uriString) {
    URI uri = URI.create(uriString);

    for (UriHandler h : handlers) {
      Optional<ClientTransport> r = h.buildClient(uri);
      if (r.isPresent()) {
        return r.get();
      }
    }

    return FAILED_CLIENT_LOOKUP;
  }

  public static ServerTransport serverForUri(String uri) {
    return UriTransportRegistry.fromServices().findServer(uri);
  }

  private ServerTransport findServer(String uriString) {
    URI uri = URI.create(uriString);

    for (UriHandler h : handlers) {
      Optional<ServerTransport> r = h.buildServer(uri);
      if (r.isPresent()) {
        return r.get();
      }
    }

    return FAILED_SERVER_LOOKUP;
  }
}
