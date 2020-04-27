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

package io.rsocket;

import org.reactivestreams.Subscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

/** An interface which allows listening to when a specific instance of this interface is closed */
public interface Closeable extends Disposable {
  /**
   * Returns a {@link Mono} that terminates when the instance is terminated by any reason. Note, in
   * case of error termination, the cause of error will be propagated as an error signal through
   * {@link org.reactivestreams.Subscriber#onError(Throwable)}. Otherwise, {@link
   * Subscriber#onComplete()} will be called.
   *
   * @return a {@link Mono} to track completion with success or error of the underlying resource.
   *     When the underlying resource is an `RSocket`, the {@code Mono} exposes stream 0 (i.e.
   *     connection level) errors.
   */
  Mono<Void> onClose();
}
