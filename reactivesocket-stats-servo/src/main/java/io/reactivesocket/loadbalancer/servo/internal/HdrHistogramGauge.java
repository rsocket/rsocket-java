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

package io.reactivesocket.loadbalancer.servo.internal;

import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.NumberGauge;
import org.HdrHistogram.Histogram;

/**
 * Gauge that wraps a {@link Histogram} and when it's polled returns a particular percentage
 */
public class HdrHistogramGauge extends NumberGauge {
    private final SlidingWindowHistogram histogram;
    private final double percentile;
    private final Runnable slide;

    public HdrHistogramGauge(MonitorConfig monitorConfig, SlidingWindowHistogram histogram, double percentile, Runnable slide) {
        super(monitorConfig);
        this.histogram = histogram;
        this.percentile = percentile;
        this.slide = slide;

        DefaultMonitorRegistry.getInstance().register(this);
    }

    @Override
    public Long getValue() {
        long value = histogram.aggregateHistogram().getValueAtPercentile(percentile);
        slide.run();
        return value;
    }
}
