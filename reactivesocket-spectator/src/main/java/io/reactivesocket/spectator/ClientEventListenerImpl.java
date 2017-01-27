/*
 * Copyright 2017 Netflix, Inc.
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

package io.reactivesocket.spectator;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Spectator;
import com.netflix.spectator.api.histogram.PercentileTimer;
import io.reactivesocket.events.ClientEventListener;

import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;

import static io.reactivesocket.spectator.internal.SpectatorUtil.*;

public class ClientEventListenerImpl extends EventListenerImpl implements ClientEventListener {

    private final Counter connectStarts;
    private final Counter connectFailed;
    private final Counter connectCancelled;
    private final Counter connectSuccess;
    private final PercentileTimer connectSuccessLatency;
    private final PercentileTimer connectFailureLatency;
    private final PercentileTimer connectCancelledLatency;

    public ClientEventListenerImpl(Registry registry, String monitorId) {
        super(registry, monitorId);
        connectStarts = registry.counter(createId(registry, "connectStart", monitorId));
        connectFailed = registry.counter(createId(registry, "connectFailed", monitorId));
        connectCancelled = registry.counter(createId(registry, "connectCancelled", monitorId));
        connectSuccess = registry.counter(createId(registry, "connectSuccess", monitorId));
        connectSuccessLatency = PercentileTimer.get(registry, createId(registry, "connectLatency", monitorId,
                                                                       "outcome", "success"));
        connectFailureLatency = PercentileTimer.get(registry, createId(registry, "connectLatency", monitorId,
                                                                       "outcome", "success"));
        connectCancelledLatency = PercentileTimer.get(registry, createId(registry, "connectLatency", monitorId,
                                                                         "outcome", "success"));
    }

    public ClientEventListenerImpl(String monitorId) {
        this(Spectator.globalRegistry(), monitorId);
    }

    @Override
    public void connectStart() {
        connectStarts.increment();
    }

    @Override
    public void connectCompleted(DoubleSupplier socketAvailabilitySupplier, long duration, TimeUnit durationUnit) {
        connectSuccess.increment();
        connectSuccessLatency.record(duration, durationUnit);
    }

    @Override
    public void connectFailed(long duration, TimeUnit durationUnit, Throwable cause) {
        connectFailed.increment();
        connectFailureLatency.record(duration, durationUnit);
    }

    @Override
    public void connectCancelled(long duration, TimeUnit durationUnit) {
        connectCancelled.increment();
        connectCancelledLatency.record(duration, durationUnit);
    }
}
