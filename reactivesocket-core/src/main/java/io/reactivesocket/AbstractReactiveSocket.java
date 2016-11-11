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

package io.reactivesocket;

import io.reactivesocket.reactivestreams.extensions.internal.EmptySubject;
import org.reactivestreams.Publisher;
import io.reactivesocket.reactivestreams.extensions.Px;

/**
 * An abstract implementation of {@link ReactiveSocket}. All request handling methods emit
 * {@link UnsupportedOperationException} and hence must be overridden to provide a valid implementation.<p>
 *
 * {@link #close()} and {@link #onClose()} returns a {@code Publisher} that never terminates.
 */
public abstract class AbstractReactiveSocket implements ReactiveSocket {

    private final EmptySubject onClose = new EmptySubject();

    @Override
    public Publisher<Void> fireAndForget(Payload payload) {
        return Px.error(new UnsupportedOperationException("Fire and forget not implemented."));
    }

    @Override
    public Publisher<Payload> requestResponse(Payload payload) {
        return Px.error(new UnsupportedOperationException("Request-Response not implemented."));
    }

    @Override
    public Publisher<Payload> requestStream(Payload payload) {
        return Px.error(new UnsupportedOperationException("Request-Stream not implemented."));
    }

    @Override
    public Publisher<Payload> requestSubscription(Payload payload) {
        return Px.error(new UnsupportedOperationException("Request-Subscription not implemented."));
    }

    @Override
    public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
        return Px.error(new UnsupportedOperationException("Request-Channel not implemented."));
    }

    @Override
    public Publisher<Void> metadataPush(Payload payload) {
        return Px.error(new UnsupportedOperationException("Metadata-Push not implemented."));
    }

    @Override
    public Publisher<Void> close() {
        return s -> {
            onClose.onComplete();
            onClose.subscribe(s);
        };
    }

    @Override
    public Publisher<Void> onClose() {
        return onClose;
    }
}
