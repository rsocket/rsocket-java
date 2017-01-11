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

import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Spectator;
import io.reactivesocket.events.ClientEventListener;
import io.reactivesocket.spectator.internal.HdrHistogramPercentileTimer;
import io.reactivesocket.spectator.internal.ThreadLocalAdderCounter;
import io.reactivesocket.util.Clock;

import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;

public class ClientEventListenerImpl extends EventListenerImpl implements ClientEventListener {

    private final ThreadLocalAdderCounter connectStarts;
    private final ThreadLocalAdderCounter connectFailed;
    private final ThreadLocalAdderCounter connectCancelled;
    private final ThreadLocalAdderCounter connectSuccess;
    private final HdrHistogramPercentileTimer connectSuccessLatency;
    private final HdrHistogramPercentileTimer connectFailureLatency;
    private final HdrHistogramPercentileTimer connectCancelledLatency;

    public ClientEventListenerImpl(Registry registry, String monitorId) {
        super(registry, monitorId);
        connectStarts = new ThreadLocalAdderCounter(registry, "connectStart", monitorId);
        connectFailed = new ThreadLocalAdderCounter(registry, "connectFailed", monitorId);
        connectCancelled = new ThreadLocalAdderCounter(registry, "connectCancelled", monitorId);
        connectSuccess = new ThreadLocalAdderCounter(registry, "connectSuccess", monitorId);
        connectSuccessLatency = new HdrHistogramPercentileTimer(registry, "connectLatency", monitorId,
                                                                "outcome", "success");
        connectFailureLatency = new HdrHistogramPercentileTimer(registry, "connectLatency", monitorId,
                                                                "outcome", "success");
        connectCancelledLatency = new HdrHistogramPercentileTimer(registry, "connectLatency", monitorId,
                                                                  "outcome", "success");
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
        connectSuccessLatency.record(Clock.unit().convert(duration, durationUnit));
    }

    @Override
    public void connectFailed(long duration, TimeUnit durationUnit, Throwable cause) {
        connectFailed.increment();
        connectFailureLatency.record(Clock.unit().convert(duration, durationUnit));
    }

    @Override
    public void connectCancelled(long duration, TimeUnit durationUnit) {
        connectCancelled.increment();
        connectCancelledLatency.record(Clock.unit().convert(duration, durationUnit));
    }
}
