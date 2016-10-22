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
import io.reactivesocket.reactivestreams.extensions.Px;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.function.LongSupplier;

public final class KeepAliveProvider {

    private volatile boolean ackThresholdBreached;
    private volatile long lastKeepAliveMillis;
    private volatile long lastAckMillis;
    private final Publisher<Long> ticks;
    private final int keepAlivePeriodMillis;
    private final int missedKeepAliveThreshold;
    private final LongSupplier currentTimeSupplier;

    private KeepAliveProvider(Publisher<Long> ticks, int keepAlivePeriodMillis, int missedKeepAliveThreshold,
                              LongSupplier currentTimeSupplier) {
        this.ticks = s -> {
            ticks.subscribe(new Subscriber<Long>() {
                private Subscription subscription;

                @Override
                public void onSubscribe(Subscription subscription) {
                    this.subscription = subscription;
                    s.onSubscribe(subscription);
                }

                @Override
                public void onNext(Long aLong) {
                    updateAckBreachThreshold();
                    if (ackThresholdBreached) {
                        onError(new ConnectionException("Missing keep alive from the peer."));
                        subscription.cancel();
                    } else {
                        lastKeepAliveMillis = currentTimeSupplier.getAsLong();
                        s.onNext(aLong);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    s.onError(t);
                }

                @Override
                public void onComplete() {
                    s.onComplete();
                }
            });
        };
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
    public Publisher<Long> ticks() {
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
        return from(Integer.MAX_VALUE, Px.never());
    }

    public static KeepAliveProvider from(int keepAlivePeriodMillis, Publisher<Long> keepAliveTicks) {
        return from(keepAlivePeriodMillis, SetupProvider.DEFAULT_MAX_KEEP_ALIVE_MISSING_ACK, keepAliveTicks);
    }

    public static KeepAliveProvider from(int keepAlivePeriodMillis, int missedKeepAliveThreshold,
                                         Publisher<Long> keepAliveTicks) {
        return from(keepAlivePeriodMillis, missedKeepAliveThreshold, keepAliveTicks, System::currentTimeMillis);
    }

    public static KeepAliveProvider from(int keepAlivePeriodMillis, int missedKeepAliveThreshold,
                                         Publisher<Long> keepAliveTicks, LongSupplier currentTimeSupplier) {
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
