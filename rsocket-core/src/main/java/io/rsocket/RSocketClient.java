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

import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/**
 * Contract to perform RSocket requests from client to server, transparently connecting and ensuring
 * a single, shared connection to make requests with.
 *
 * <p>{@code RSocketClient} contains a {@code Mono<RSocket>} {@link #source() source}. It uses it to
 * obtain a live, shared {@link RSocket} connection on the first request and on subsequent requests
 * if the connection is lost. This eliminates the need to obtain a connection first, and makes it
 * easy to pass a single {@code RSocketClient} to use from multiple places.
 *
 * <p>Request methods of {@code RSocketClient} allow multiple subscriptions with each subscription
 * performing a new request. Therefore request methods accept {@code Mono<Payload>} rather than
 * {@code Payload} as on {@link RSocket}. By contrast, {@link RSocket} request methods cannot be
 * subscribed to more than once.
 *
 * <p>Use {@link io.rsocket.core.RSocketConnector RSocketConnector} to create a client:
 *
 * <pre class="code">{@code
 * RSocketClient client =
 *         RSocketConnector.create()
 *                 .metadataMimeType("message/x.rsocket.composite-metadata.v0")
 *                 .dataMimeType("application/cbor")
 *                 .toRSocketClient(TcpClientTransport.create("localhost", 7000));
 * }</pre>
 *
 * <p>Use the {@link io.rsocket.core.RSocketConnector#reconnect(Retry) RSocketConnector#reconnect}
 * method to configure the retry logic to use whenever a shared {@code RSocket} connection needs to
 * be obtained:
 *
 * <pre class="code">{@code
 * RSocketClient client =
 *         RSocketConnector.create()
 *                 .metadataMimeType("message/x.rsocket.composite-metadata.v0")
 *                 .dataMimeType("application/cbor")
 *                 .reconnect(Retry.fixedDelay(3, Duration.ofSeconds(1)))
 *                 .toRSocketClient(TcpClientTransport.create("localhost", 7000));
 * }</pre>
 *
 * @since 1.0.1
 */
public interface RSocketClient extends Disposable {

  /** Return the underlying source used to obtain a shared {@link RSocket} connection. */
  Mono<RSocket> source();

  /**
   * Perform a Fire-and-Forget interaction via {@link RSocket#fireAndForget(Payload)}. Allows
   * multiple subscriptions and performs a request per subscriber.
   */
  Mono<Void> fireAndForget(Mono<Payload> payloadMono);

  /**
   * Perform a Request-Response interaction via {@link RSocket#requestResponse(Payload)}. Allows
   * multiple subscriptions and performs a request per subscriber.
   */
  Mono<Payload> requestResponse(Mono<Payload> payloadMono);

  /**
   * Perform a Request-Stream interaction via {@link RSocket#requestStream(Payload)}. Allows
   * multiple subscriptions and performs a request per subscriber.
   */
  Flux<Payload> requestStream(Mono<Payload> payloadMono);

  /**
   * Perform a Request-Channel interaction via {@link RSocket#requestChannel(Publisher)}. Allows
   * multiple subscriptions and performs a request per subscriber.
   */
  Flux<Payload> requestChannel(Publisher<Payload> payloads);

  /**
   * Perform a Metadata Push via {@link RSocket#metadataPush(Payload)}. Allows multiple
   * subscriptions and performs a request per subscriber.
   */
  Mono<Void> metadataPush(Mono<Payload> payloadMono);
}
