/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A contract providing different interaction models for <a
 * href="https://github.com/RSocket/reactivesocket/blob/master/Protocol.md">RSocket protocol</a>.
 */
public interface RSocket extends Availability, Closeable {

  /**
   * Fire and Forget interaction model of {@code RSocket}.
   *
   * @param payload Request payload.
   * @return {@code Publisher} that completes when the passed {@code payload} is successfully
   *     handled, otherwise errors.
   */
  Mono<Void> fireAndForget(Payload payload);

  /**
   * Request-Response interaction model of {@code RSocket}.
   *
   * @param payload Request payload.
   * @return {@code Publisher} containing at most a single {@code Payload} representing the
   *     response.
   */
  Mono<Payload> requestResponse(Payload payload);

  /**
   * Request-Stream interaction model of {@code RSocket}.
   *
   * @param payload Request payload.
   * @return {@code Publisher} containing the stream of {@code Payload}s representing the response.
   */
  Flux<Payload> requestStream(Payload payload);

  /**
   * Request-Channel interaction model of {@code RSocket}.
   *
   * @param payloads Stream of request payloads.
   * @return Stream of response payloads.
   */
  Flux<Payload> requestChannel(Publisher<Payload> payloads);

  /**
   * Metadata-Push interaction model of {@code RSocket}.
   *
   * @param payload Request payloads.
   * @return {@code Publisher} that completes when the passed {@code payload} is successfully
   *     handled, otherwise errors.
   */
  Mono<Void> metadataPush(Payload payload);

  @Override
  default double availability() {
    return 0.0;
  }
}
