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

package io.reactivesocket.internal;

import org.reactivestreams.Subscription;

import java.util.function.LongConsumer;

/**
 * A factory for implementations of {@link Subscription}
 */
public final class Subscriptions {

    private static final Subscription EMPTY = new Subscription() {
        @Override
        public void request(long n) {
            // No Op
        }

        @Override
        public void cancel() {
            // No Op
        }
    };

    private Subscriptions() {
        // No instances.
    }

    /**
     * Empty {@code Subscription} i.e. it does nothing, all method implementations are no-op.
     *
     * @return An empty {@code Subscription}. This will be a shared instance.
     */
    public static Subscription empty() {
        return EMPTY;
    }

    /**
     * Creates a new {@code Subscription} object that invokes the passed {@code onCancelAction} when the subscription is
     * cancelled. This will ignore {@link Subscription#request(long)} calls to the returned {@code Subscription}
     *
     * @return A new {@code Subscription} instance.
     */
    public static Subscription forCancel(Runnable onCancelAction) {
        return new Subscription() {
            @Override
            public void request(long n) {
                // Do nothing.
            }

            @Override
            public void cancel() {
                onCancelAction.run();
            }
        };
    }

    /**
     * Creates a new {@code Subscription} object that invokes the passed {@code requestN} consumer for every call to
     * the returned {@link Subscription#request(long)} and ignores {@link Subscription#cancel()} calls to the returned
     * {@code Subscription}
     *
     * @return A new {@code Subscription} instance.
     */
    public static Subscription forRequestN(LongConsumer requestN) {
        return new Subscription() {
            @Override
            public void request(long n) {
                requestN.accept(n);
            }

            @Override
            public void cancel() {
                // No op
            }
        };
    }

    /**
     * Creates a new {@code Subscription} object that invokes the passed {@code requestN} consumer for every call to
     * the returned {@link Subscription#request(long)} and {@code onCancelAction} for every call to the returned
     * {@link Subscription#cancel()}
     *
     * @return A new {@code Subscription} instance.
     */
    public static Subscription create(LongConsumer requestN, Runnable onCancelAction) {
        return new Subscription() {
            @Override
            public void request(long n) {
                requestN.accept(n);
            }

            @Override
            public void cancel() {
                onCancelAction.run();
            }
        };
    }
}
