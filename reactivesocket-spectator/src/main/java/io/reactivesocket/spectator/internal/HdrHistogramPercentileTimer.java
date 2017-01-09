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
package io.reactivesocket.spectator.internal;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Spectator;

import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleFunction;

/**
 * Captures a HdrHistogram and sends it to pre-defined Server Counters.
 * The buckets are min, max, 50%, 90%, 99%, 99.9%, and 99.99%
 */
public class HdrHistogramPercentileTimer {
    private final SlidingWindowHistogram histogram = new SlidingWindowHistogram();

    private static final long TIMEOUT = TimeUnit.MINUTES.toMillis(1);

    private volatile long lastCleared = System.currentTimeMillis();

    public HdrHistogramPercentileTimer(Registry registry, String name, String monitorId) {
        registerGauge(name, monitorId, registry, "min", timer -> getMin());
        registerGauge(name, monitorId, registry, "max", timer -> getMax());
        registerGauge(name, monitorId, registry, "50", timer -> getP50());
        registerGauge(name, monitorId, registry, "90", timer -> getP90());
        registerGauge(name, monitorId, registry, "99", timer -> getP99());
        registerGauge(name, monitorId, registry, "99.9", timer -> getP99_9());
        registerGauge(name, monitorId, registry, "99.99", timer -> getP99_99());
    }

    public HdrHistogramPercentileTimer(String name, String monitorId) {
        this(Spectator.globalRegistry(), name, monitorId);
    }

    /**
     * Records a value for to the histogram and updates the Servo counter buckets
     *
     * @param value the value to update
     */
    public void record(long value) {
        histogram.recordValue(value);
    }

    public Long getMin() {
        return histogram.aggregateHistogram().getMinValue();
    }

    public Long getMax() {
        return histogram.aggregateHistogram().getMaxValue();
    }

    public Long getP50() {
        return getPercentile(50);
    }

    public Long getP90() {
        return getPercentile(90);
    }

    public Long getP99() {
        return getPercentile(99);
    }

    public Long getP99_9() {
        return getPercentile(99.9);
    }

    public Long getP99_99() {
        return getPercentile(99.99);
    }

    private synchronized void slide() {
        if (System.currentTimeMillis() - lastCleared > TIMEOUT) {
            histogram.rotateHistogram();
            lastCleared = System.currentTimeMillis();
        }
    }

    private Long getPercentile(double percentile) {
        slide();
        return histogram.aggregateHistogram().getValueAtPercentile(percentile);
    }

    private void registerGauge(String metricName, String monitorId, Registry registry, String percentileTag,
                               ToDoubleFunction<HdrHistogramPercentileTimer> function) {
        Id id = registry.createId(metricName, "id", monitorId, "value", percentileTag);
        registry.gauge(id, this, function);
    }
}