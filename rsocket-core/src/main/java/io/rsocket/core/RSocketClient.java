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
package io.rsocket.core;

import io.rsocket.Closeable;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Contract for performing RSocket requests.
 *
 * <p>{@link RSocketClient} differs from {@link RSocket} in a number of ways:
 *
 * <ul>
 *   <li>{@code RSocket} represents a "live" connection that is transient and needs to be obtained
 *       typically from a {@code Mono<RSocket>} source via {@code flatMap} or block. By contrast,
 *       {@code RSocketClient} is a higher level layer that contains such a {@link #source() source}
 *       of connections and transparently obtains and re-obtains a shared connection as needed when
 *       requests are made concurrently. That means an {@code RSocketClient} can simply be created
 *       once, even before a connection is established, and shared as a singleton across multiple
 *       places as you would with any other client.
 *   <li>For request input {@code RSocket} accepts an instance of {@code Payload} and does not allow
 *       more than one subscription per request because there is no way to safely re-use that input.
 *       By contrast {@code RSocketClient} accepts {@code Publisher<Payload>} and allow
 *       re-subscribing which repeats the request.
 *   <li>{@code RSocket} can be used for sending and it can also be implemented for receiving. By
 *       contrast {@code RSocketClient} is used only for sending, typically from the client side
 *       which allows obtaining and re-obtaining connections from a source as needed. However it can
 *       also be used from the server side by {@link #from(RSocket) wrapping} the "live" {@code
 *       RSocket} for a given connection.
 * </ul>
 *
 * <p>The example below shows how to create an {@code RSocketClient}:
 *
 * <pre class="code">{@code
 * Mono<RSocket> source =
 *         RSocketConnector.create()
 *                 .metadataMimeType("message/x.rsocket.composite-metadata.v0")
 *                 .dataMimeType("application/cbor")
 *                 .connect(TcpClientTransport.create("localhost", 7000));
 *
 * RSocketClient client = RSocketClient.from(source);
 * }</pre>
 *
 * <p>The below configures retry logic to use when a shared {@code RSocket} connection is obtained:
 *
 * <pre class="code">{@code
 * Mono<RSocket> source =
 *         RSocketConnector.create()
 *                 .metadataMimeType("message/x.rsocket.composite-metadata.v0")
 *                 .dataMimeType("application/cbor")
 *                 .reconnect(Retry.fixedDelay(3, Duration.ofSeconds(1)))
 *                 .connect(TcpClientTransport.create("localhost", 7000));
 *
 * RSocketClient client = RSocketClient.from(source);
 * }</pre>
 *
 * @since 1.1
 * @see io.rsocket.loadbalance.LoadbalanceRSocketClient
 */
public interface RSocketClient extends Closeable {

  /**
   * Connect to the remote rsocket endpoint, if not yet connected. This method is a shortcut for
   * {@code RSocketClient#source().subscribe()}.
   *
   * @return {@code true} if an attempt to connect was triggered or if already connected, or {@code
   *     false} if the client is terminated.
   */
  default boolean connect() {
    throw new NotImplementedException();
  }

  default Mono<Void> onClose() {
    return Mono.error(new NotImplementedException());
  }

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

  /**
   * Create an {@link RSocketClient} that obtains shared connections as needed, when requests are
   * made, from the given {@code Mono<RSocket>} source.
   *
   * @param source the source for connections, typically prepared via {@link RSocketConnector}.
   * @return the created client instance
   */
  static RSocketClient from(Mono<RSocket> source) {
    return new DefaultRSocketClient(source);
  }

  /**
   * Adapt the given {@link RSocket} to use as {@link RSocketClient}. This is useful to wrap the
   * sending {@code RSocket} in a server.
   *
   * <p><strong>Note:</strong> unlike an {@code RSocketClient} created via {@link
   * RSocketClient#from(Mono)}, the instance returned from this factory method can only perform
   * requests for as long as the given {@code RSocket} remains "live".
   *
   * @param rsocket the {@code RSocket} to perform requests with
   * @return the created client instance
   */
  static RSocketClient from(RSocket rsocket) {
    return new RSocketClientAdapter(rsocket);
  }
}
