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

package io.rsocket.transport;

import io.rsocket.DuplexConnection;
import reactor.core.publisher.Mono;

/** A client contract for writing transports of RSocket. */
public interface ClientTransport extends Transport {

  /**
   * Returns a {@code Publisher}, every subscription to which returns a single {@code
   * DuplexConnection}.
   *
   * @return {@code Publisher}, every subscription returns a single {@code DuplexConnection}.
   */
  Mono<DuplexConnection> connect();
}
