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
import reactor.core.publisher.MonoProcessor;

/**
 * An abstract implementation of {@link ReactiveSocket}. All request handling methods emit
 * {@link UnsupportedOperationException} and hence must be overridden to provide a valid implementation.<p>
 *
 * {@link #close()} and {@link #onClose()} returns a {@code Publisher} that never terminates.
 */
public abstract class AbstractReactiveSocket implements ReactiveSocket {

    private final MonoProcessor<Void> onClose = MonoProcessor.create();

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
        return Mono.error(new UnsupportedOperationException("Fire and forget not implemented."));
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
        return Mono.error(new UnsupportedOperationException("Request-Response not implemented."));
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
        return Flux.error(new UnsupportedOperationException("Request-Stream not implemented."));
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
        return Flux.error(new UnsupportedOperationException("Request-Channel not implemented."));
    }

    @Override
    public Mono<Void> metadataPush(Payload payload) {
        return Mono.error(new UnsupportedOperationException("Metadata-Push not implemented."));
    }

    @Override
    public Mono<Void> close() {
        return Mono.defer(() -> {
            onClose.onComplete();
            return onClose;
        });
    }

    @Override
    public Mono<Void> onClose() {
        return onClose;
    }
}
