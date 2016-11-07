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

import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;

/**
 * {@link EventListener} for a client.
 */
public interface ClientEventListener extends EventListener {

    /**
     * Event when a new connection is initiated.
     */
    default void connectStart() {}

    /**
     * Event when a connection is successfully completed.
     *
     * @param socketAvailabilitySupplier A supplier for the availability of the connected socket.
     * @param duration Time taken since connection initiation and completion.
     * @param durationUnit {@code TimeUnit} for the duration.
     */
    default void connectCompleted(DoubleSupplier socketAvailabilitySupplier, long duration, TimeUnit durationUnit) {}

    /**
     * Event when a connection attempt fails.
     *
     * @param duration Time taken since connection initiation and failure.
     * @param durationUnit {@code TimeUnit} for the duration.
     * @param cause Cause for the failure.
     */
    default void connectFailed(long duration, TimeUnit durationUnit, Throwable cause) {}
}
