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
import org.reactivestreams.Subscriber;
import io.reactivesocket.reactivestreams.extensions.Px;

import java.nio.channels.ClosedChannelException;

/**
 * Represents a connection with input/output that the protocol uses. 
 */
public interface DuplexConnection extends Availability {

    /**
     * Sends the source of {@link Frame}s on this connection and returns the {@code Publisher} representing the result
     * of this send.
     *
     * <h2>Flow control</h2>
     *
     * The passed {@code Publisher} must
     *
     * @param frame Stream of {@code Frame}s to send on the connection.
     *
     * @return {@code Publisher} that completes when all the frames are written on the connection successfully and
     * errors when it fails.
     */
    Publisher<Void> send(Publisher<Frame> frame);

    /**
     * Sends a single {@code Frame} on this connection and returns the {@code Publisher} representing the result
     * of this send.
     *
     * @param frame {@code Frame} to send.
     *
     * @return {@code Publisher} that completes when the frame is written on the connection successfully and errors
     * when it fails.
     */
    default Publisher<Void> sendOne(Frame frame) {
        return send(Px.just(frame));
    }

    /**
     * Returns a stream of all {@code Frame}s received on this connection.
     *
     * <h2>Completion</h2>
     *
     * Returned {@code Publisher} <em>MUST</em> never emit a completion event ({@link Subscriber#onComplete()}.
     *
     * <h2>Error</h2>
     *
     * Returned {@code Publisher} can error with various transport errors. If the underlying physical connection is
     * closed by the peer, then the returned stream from here <em>MUST</em> emit an {@link ClosedChannelException}.
     *
     * <h2>Multiple Subscriptions</h2>
     *
     * Returned {@code Publisher} is not required to support multiple concurrent subscriptions. ReactiveSocket will
     * never have multiple subscriptions to this source. Implementations <em>MUST</em> emit an
     * {@link IllegalStateException} for subsequent concurrent subscriptions, if they do not support multiple
     * concurrent subscriptions.
     *
     * @return Stream of all {@code Frame}s received.
     */
    Publisher<Frame> receive();

    /**
     * Close this {@code DuplexConnection} upon subscribing to the returned {@code Publisher}
     *
     * <em>This method is idempotent and hence can be called as many times at any point with same outcome.</em>
     *
     * @return A {@code Publisher} that completes when this {@code DuplexConnection} close is complete.
     */
    Publisher<Void> close();

    /**
     * Returns a {@code Publisher} that completes when this {@code DuplexConnection} is closed.
     *
     * @return A {@code Publisher} that completes when this {@code DuplexConnection} close is complete.
     */
    Publisher<Void> onClose();
}
