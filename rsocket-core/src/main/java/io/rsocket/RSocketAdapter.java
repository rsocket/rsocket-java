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

/**
 * Package private class with default implementations for use in {@link RSocket}. The main purpose
 * is to hide static {@link UnsupportedOperationException} declarations.
 */
class RSocketAdapter {

  private static final Mono<Void> UNSUPPORTED_FIRE_AND_FORGET =
      Mono.error(new UnsupportedOperationException("Fire-and-Forget not implemented."));

  private static final Mono<Payload> UNSUPPORTED_REQUEST_RESPONSE =
      Mono.error(new UnsupportedOperationException("Request-Response not implemented."));

  private static final Flux<Payload> UNSUPPORTED_REQUEST_STREAM =
      Flux.error(new UnsupportedOperationException("Request-Stream not implemented."));

  private static final Flux<Payload> UNSUPPORTED_REQUEST_CHANNEL =
      Flux.error(new UnsupportedOperationException("Request-Channel not implemented."));

  private static final Mono<Void> UNSUPPORTED_METADATA_PUSH =
      Mono.error(new UnsupportedOperationException("Metadata-Push not implemented."));

  static Mono<Void> fireAndForget(Payload payload) {
    payload.release();
    return RSocketAdapter.UNSUPPORTED_FIRE_AND_FORGET;
  }

  static Mono<Payload> requestResponse(Payload payload) {
    payload.release();
    return RSocketAdapter.UNSUPPORTED_REQUEST_RESPONSE;
  }

  static Flux<Payload> requestStream(Payload payload) {
    payload.release();
    return RSocketAdapter.UNSUPPORTED_REQUEST_STREAM;
  }

  static Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return RSocketAdapter.UNSUPPORTED_REQUEST_CHANNEL;
  }

  static Mono<Void> metadataPush(Payload payload) {
    payload.release();
    return RSocketAdapter.UNSUPPORTED_METADATA_PUSH;
  }
}
