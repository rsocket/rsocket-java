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

package io.rsocket;

import reactor.core.publisher.Mono;

/** */
public interface Closeable {
  /**
   * Close this {@code RSocket} upon subscribing to the returned {@code Publisher}
   *
   * <p><em>This method is idempotent and hence can be called as many times at any point with same
   * outcome.</em>
   *
   * @return A {@code Publisher} that triggers the close when subscribed to and that completes when
   *     this {@code RSocket} close is complete.
   */
  Mono<Void> close();

  /**
   * Returns a {@code Publisher} that completes when this {@code RSocket} is closed. A {@code
   * RSocket} can be closed by explicitly calling {@link #close()} or when the underlying transport
   * connection is closed.
   *
   * @return A {@code Publisher} that completes when this {@code RSocket} close is complete.
   */
  Mono<Void> onClose();
}
