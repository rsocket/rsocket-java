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

package io.rsocket.spectator;

import com.netflix.spectator.api.Registry;
import io.rsocket.RSocket;
import io.rsocket.plugins.RSocketInterceptor;

/** Interceptor that wraps a {@link RSocket} with a {@link SpectatorRSocket} */
public class SpectatorRSocketInterceptor implements RSocketInterceptor {
  private static final String[] EMPTY = new String[0];
  private final Registry registry;
  private final String[] tags;

  public SpectatorRSocketInterceptor(Registry registry, String... tags) {
    this.registry = registry;
    this.tags = tags;
  }

  public SpectatorRSocketInterceptor(Registry registry) {
    this(registry, EMPTY);
  }

  @Override
  public RSocket apply(RSocket reactiveSocket) {
    return new SpectatorRSocket(registry, reactiveSocket, tags);
  }
}
