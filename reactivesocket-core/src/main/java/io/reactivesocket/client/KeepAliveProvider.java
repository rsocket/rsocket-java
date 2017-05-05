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

package io.reactivesocket.client;

import io.reactivesocket.exceptions.ConnectionException;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.function.LongSupplier;

/**
 * A provider of keep-alive ticks that are sent from a client to a server over a ReactiveSocket connection.
 * {@link #ticks()} provides a source that emits an item whenever a keep-alive frame is to be sent. This expects to
 * receive an acknowledgment from the peer for every keep-alive frame sent, in absence of a configurable number of
 * consecutive missed acknowledgments, it will generate an error from the {@link #ticks()} source.
 */
public final class KeepAliveProvider {

    private volatile boolean ackThresholdBreached;
    private volatile long lastKeepAliveMillis;
    private volatile long lastAckMillis;
    private final Flux<Long> ticks;
    private final int keepAlivePeriodMillis;
    private final int missedKeepAliveThreshold;
    private final LongSupplier currentTimeSupplier;

    private KeepAliveProvider(Flux<Long> ticks, int keepAlivePeriodMillis, int missedKeepAliveThreshold,
                              LongSupplier currentTimeSupplier) {
        this.ticks = ticks.map(tick -> {
            updateAckBreachThreshold();
            if (ackThresholdBreached) {
                throw new ConnectionException("Missing keep alive from the peer.");
            } else {
                lastKeepAliveMillis = currentTimeSupplier.getAsLong();
                return tick;
            }
        });
        this.keepAlivePeriodMillis = keepAlivePeriodMillis;
        this.missedKeepAliveThreshold = missedKeepAliveThreshold;
        this.currentTimeSupplier = currentTimeSupplier;
    }

    /**
     * Source of ticks at which a keep-alive frame must be send to the peer. This expects a call to {@link #ack()} when
     * an acknowledgment for each keep-alive frame is received from the peer. In absence of
     * {@link #getMissedKeepAliveThreshold()} consecutive failures to receive an ack, this source will emit an error.
     *
     * @return Source of keep-alive ticks.
     */
    public Flux<Long> ticks() {
        return ticks;
    }

    /**
     * Invoked on receipt of an acknowledgment of keep-alive from the peer.
     */
    public void ack() {
        lastAckMillis = currentTimeSupplier.getAsLong();
        updateAckBreachThreshold();
    }

    /**
     * Time between two keep-alive ticks.
     *
     * @return Time between two keep-alive ticks.
     */
    public int getKeepAlivePeriodMillis() {
        return keepAlivePeriodMillis;
    }

    /**
     * Number of consecutive keep-alive that are not acknowledged by the peer.
     *
     * @return Number of consecutive keep-alive that are not acknowledged by the peer.
     */
    public int getMissedKeepAliveThreshold() {
        return missedKeepAliveThreshold;
    }

    /**
     * Creates a new {@link KeepAliveProvider} that never sends a keep-alive frame.
     *
     * @return A new {@link KeepAliveProvider} that never sends a keep-alive frame.
     */
    public static KeepAliveProvider never() {
        return from(Integer.MAX_VALUE, Integer.MAX_VALUE, Flux.never());
    }

    /**
     * Creates a new {@link KeepAliveProvider} that sends a keep alive frame every {@code keepAlivePeriodMillis}
     * milliseconds.
     *
     * @param keepAlivePeriodMillis Duration in milliseconds after which a keep-alive frame is sent.
     *
     * @return A new {@link KeepAliveProvider} that sends periodic keep-alive frames.
     */
    public static KeepAliveProvider from(int keepAlivePeriodMillis) {
        return from(keepAlivePeriodMillis, SetupProvider.DEFAULT_MAX_KEEP_ALIVE_MISSING_ACK,
                Flux.interval(Duration.ofMillis(keepAlivePeriodMillis)));
    }

    /**
     * Creates a new {@link KeepAliveProvider} that sends a keep alive frame every {@code keepAlivePeriodMillis}
     * milliseconds. The created provider will tolerate a maximum of {@code missedKeepAliveThreshold} consecutive
     * acknowledgments from the peer, before generating an error from {@link #ticks()}
     *
     * @param keepAlivePeriodMillis Duration in milliseconds after which a keep-alive frame is sent.
     * @param missedKeepAliveThreshold Maximum concurrent missed acknowledgements for keep-alives from the peer.
     * @param keepAliveTicks A source which emits an item whenever a keepa-live frame is to be sent.
     *
     * @return A new {@link KeepAliveProvider} that sends periodic keep-alive frames.
     */
    public static KeepAliveProvider from(int keepAlivePeriodMillis, int missedKeepAliveThreshold,
                                         Flux<Long> keepAliveTicks) {
        return from(keepAlivePeriodMillis, missedKeepAliveThreshold, keepAliveTicks, System::currentTimeMillis);
    }

    /**
     * Creates a new {@link KeepAliveProvider} that sends a keep alive frame every {@code keepAlivePeriodMillis}
     * milliseconds. The created provider will tolerate a maximum of {@code missedKeepAliveThreshold} consecutive
     * acknowledgments from the peer, before generating an error from {@link #ticks()}
     *
     * @param keepAlivePeriodMillis Duration in milliseconds after which a keep-alive frame is sent.
     * @param missedKeepAliveThreshold Maximum concurrent missed acknowledgements for keep-alives from the peer.
     * @param keepAliveTicks A source which emits an item whenever a keepa-live frame is to be sent.
     * @param currentTimeSupplier Supplier for the current system time.
     *
     * @return A new {@link KeepAliveProvider} that sends periodic keep-alive frames.
     */
    static KeepAliveProvider from(int keepAlivePeriodMillis, int missedKeepAliveThreshold,
                                         Flux<Long> keepAliveTicks, LongSupplier currentTimeSupplier) {
        return new KeepAliveProvider(keepAliveTicks, keepAlivePeriodMillis, missedKeepAliveThreshold,
                                     currentTimeSupplier);
    }

    private void updateAckBreachThreshold() {
        long missedAcks = (lastAckMillis - lastKeepAliveMillis) / keepAlivePeriodMillis;
        if (missedAcks < 0 || missedAcks > missedKeepAliveThreshold) {
            ackThresholdBreached = true;
        }
    }
}
