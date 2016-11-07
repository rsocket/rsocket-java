/*
 * Copyright 2016 Netflix, Inc.
 * <p>
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *  <p>
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  <p>
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations under the License.
 */

package io.reactivesocket.events;

/**
 * An event source that accepts an {@link EventListener}s on which events are dispatched.
 *
 * @param <T> Type of the {@link EventListener}
 */
public interface EventSource<T extends EventListener> {

    /**
     * Registers the passed {@code listener} to this source.
     *
     * @param listener Listener to register.
     *
     * @return A subscription which can be used to cancel this listeners interest in the source.
     *
     * @throws IllegalStateException If the source does not accept this subscription.
     */
    EventSubscription subscribe(T listener);

    /**
     * A subscription of an {@link EventListener} to an {@link EventSource}.
     */
    interface EventSubscription {

        /**
         * Cancels the registration of the associated listener to the source.
         */
        void cancel();

    }
}
