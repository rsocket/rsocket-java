/*
 * Copyright 2015-2020 the original author or authors.
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

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.net.SocketAddress;

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
  default Mono<Void> fireAndForget(Payload payload) {
    return RSocketAdapter.fireAndForget(payload);
  }

  /**
   * Request-Response interaction model of {@code RSocket}.
   *
   * @param payload Request payload.
   * @return {@code Publisher} containing at most a single {@code Payload} representing the
   *     response.
   */
  default Mono<Payload> requestResponse(Payload payload) {
    return RSocketAdapter.requestResponse(payload);
  }

  /**
   * Request-Stream interaction model of {@code RSocket}.
   *
   * @param payload Request payload.
   * @return {@code Publisher} containing the stream of {@code Payload}s representing the response.
   */
  default Flux<Payload> requestStream(Payload payload) {
    return RSocketAdapter.requestStream(payload);
  }

  /**
   * Request-Channel interaction model of {@code RSocket}.
   *
   * @param payloads Stream of request payloads.
   * @return Stream of response payloads.
   */
  default Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return RSocketAdapter.requestChannel(payloads);
  }

  /**
   * Metadata-Push interaction model of {@code RSocket}.
   *
   * @param payload Request payloads.
   * @return {@code Publisher} that completes when the passed {@code payload} is successfully
   *     handled, otherwise errors.
   */
  default Mono<Void> metadataPush(Payload payload) {
    return RSocketAdapter.metadataPush(payload);
  }

  /**
   * Returns the local address where this channel is bound to.  The returned
   * {@link SocketAddress} is supposed to be down-cast into more concrete
   * type such as {@link java.net.InetSocketAddress} to retrieve the detailed
   * information.
   *
   * @return the local address of this channel.
   *         {@code null} if this channel is not bound.
   * @since 1.1.1
   */
  default SocketAddress localAddress() {
    return null;
  }

  /**
   * Returns the remote address where this channel is connected to.  The
   * returned {@link SocketAddress} is supposed to be down-cast into more
   * concrete type such as {@link java.net.InetSocketAddress} to retrieve the detailed
   * information.
   *
   * @return the remote address of this channel.
   *         {@code null} if this channel is not connected.
   *         If this channel is not connected but it can receive messages
   *         from arbitrary remote addresses to determine
   *         the origination of the received message as this method will
   *         return {@code null}.
   * @since 1.1.1
   */
  default SocketAddress remoteAddress() {
    return null;
  }

  @Override
  default double availability() {
    return isDisposed() ? 0.0 : 1.0;
  }

  @Override
  default void dispose() {}

  @Override
  default boolean isDisposed() {
    return false;
  }

  @Override
  default Mono<Void> onClose() {
    return Mono.never();
  }
}
