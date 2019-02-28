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
      (mtu) -> Mono.error(new UnsupportedOperationException());
  private static final ServerTransport FAILED_SERVER_LOOKUP =
      (acceptor, mtu) -> Mono.error(new UnsupportedOperationException());

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

  public static ServerTransport serverForUri(String uri) {
    return UriTransportRegistry.fromServices().findServer(uri);
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
