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

import io.rsocket.Payload;
import io.rsocket.RSocket;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Simple adapter from {@link RSocket} to {@link RSocketClient}. This is useful in code that needs
 * to deal with both in the same way. When connecting to a server, typically {@link RSocketClient}
 * is expected to be used, but in a responder (client or server), it is necessary to interact with
 * {@link RSocket} to make requests to the remote end.
 *
 * @since 1.1
 */
class RSocketClientAdapter implements RSocketClient {

  private final RSocket rsocket;

  public RSocketClientAdapter(RSocket rsocket) {
    this.rsocket = rsocket;
  }

  public RSocket rsocket() {
    return rsocket;
  }

  @Override
  public Mono<RSocket> source() {
    return Mono.just(rsocket);
  }

  @Override
  public Mono<Void> onClose() {
    return rsocket.onClose();
  }

  @Override
  public Mono<Void> fireAndForget(Mono<Payload> payloadMono) {
    return payloadMono.flatMap(rsocket::fireAndForget);
  }

  @Override
  public Mono<Payload> requestResponse(Mono<Payload> payloadMono) {
    return payloadMono.flatMap(rsocket::requestResponse);
  }

  @Override
  public Flux<Payload> requestStream(Mono<Payload> payloadMono) {
    return payloadMono.flatMapMany(rsocket::requestStream);
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return rsocket.requestChannel(payloads);
  }

  @Override
  public Mono<Void> metadataPush(Mono<Payload> payloadMono) {
    return payloadMono.flatMap(rsocket::metadataPush);
  }

  @Override
  public void dispose() {
    rsocket.dispose();
  }
}
