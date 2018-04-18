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

import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import java.net.URI;
import java.util.Optional;
import java.util.ServiceLoader;

/** Maps a {@link URI} to a {@link ClientTransport} or {@link ServerTransport}. */
public interface UriHandler {

  /**
   * Load all registered instances of {@code UriHandler}.
   *
   * @return all registered instances of {@code UriHandler}
   */
  static ServiceLoader<UriHandler> loadServices() {
    return ServiceLoader.load(UriHandler.class);
  }

  /**
   * Returns an implementation of {@link ClientTransport} unambiguously mapped to a {@link URI},
   * otherwise {@link Optional#EMPTY}.
   *
   * @param uri the uri to map
   * @return an implementation of {@link ClientTransport} unambiguously mapped to a {@link URI}, *
   *     otherwise {@link Optional#EMPTY}
   * @throws NullPointerException if {@code uri} is {@code null}
   */
  Optional<ClientTransport> buildClient(URI uri);

  /**
   * Returns an implementation of {@link ServerTransport} unambiguously mapped to a {@link URI},
   * otherwise {@link Optional#EMPTY}.
   *
   * @param uri the uri to map
   * @return an implementation of {@link ServerTransport} unambiguously mapped to a {@link URI}, *
   *     otherwise {@link Optional#EMPTY}
   * @throws NullPointerException if {@code uri} is {@code null}
   */
  Optional<ServerTransport> buildServer(URI uri);
}
