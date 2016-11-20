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
 * Gauge that wraps a {@link Histogram} and when its polled returns it's min
 */
public class HdrHistogramMinGauge extends NumberGauge {
    private final SlidingWindowHistogram histogram;

    public HdrHistogramMinGauge(MonitorConfig monitorConfig, SlidingWindowHistogram histogram) {
        super(monitorConfig);
        this.histogram = histogram;

        DefaultMonitorRegistry.getInstance().register(this);
    }

    @Override
    public Long getValue() {
        return histogram.aggregateHistogram().getMinValue();
    }
}
