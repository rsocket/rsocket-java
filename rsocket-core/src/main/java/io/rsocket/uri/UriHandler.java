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
