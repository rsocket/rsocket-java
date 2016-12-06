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

import org.reactivestreams.Publisher;

/**
 * A contract providing different interaction models for <a href="https://github.com/ReactiveSocket/reactivesocket/blob/master/Protocol.md">ReactiveSocket protocol</a>.
 */
public interface ReactiveSocket extends Availability {

    /**
     * Fire and Forget interaction model of {@code ReactiveSocket}.
     *
     * @param payload Request payload.
     *
     * @return {@code Publisher} that completes when the passed {@code payload} is successfully handled, otherwise errors.
     */
    Publisher<Void> fireAndForget(Payload payload);

    /**
     * Request-Response interaction model of {@code ReactiveSocket}.
     *
     * @param payload Request payload.
     *
     * @return {@code Publisher} containing at most a single {@code Payload} representing the response.
     */
    Publisher<Payload> requestResponse(Payload payload);

    /**
     * Request-Stream interaction model of {@code ReactiveSocket}.
     *
     * @param payload Request payload.
     *
     * @return {@code Publisher} containing the stream of {@code Payload}s representing the response.
     */
    Publisher<Payload> requestStream(Payload payload);

    Publisher<Payload> requestSubscription(Payload payload);

    /**
     * Request-Channel interaction model of {@code ReactiveSocket}.
     *
     * @param payloads Stream of request payloads.
     *
     * @return Stream of response payloads.
     */
    Publisher<Payload> requestChannel(Publisher<Payload> payloads);

    /**
     * Metadata-Push interaction model of {@code ReactiveSocket}.
     *
     * @param payload Request payloads.
     *
     * @return {@code Publisher} that completes when the passed {@code payload} is successfully handled, otherwise errors.
     */
    Publisher<Void> metadataPush(Payload payload);

    @Override
    default double availability() {
        return 0.0;
    }

    /**
     * Close this {@code ReactiveSocket} upon subscribing to the returned {@code Publisher}
     *
     * <em>This method is idempotent and hence can be called as many times at any point with same outcome.</em>
     *
     * @return A {@code Publisher} that completes when this {@code ReactiveSocket} close is complete.
     */
    Publisher<Void> close();

    /**
     * Returns a {@code Publisher} that completes when this {@code ReactiveSocket} is closed. A {@code ReactiveSocket}
     * can be closed by explicitly calling {@link #close()} or when the underlying transport connection is closed.
     *
     * @return A {@code Publisher} that completes when this {@code ReactiveSocket} close is complete.
     */
    Publisher<Void> onClose();
}
