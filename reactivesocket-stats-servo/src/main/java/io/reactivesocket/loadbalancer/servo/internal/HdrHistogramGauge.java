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

import java.util.concurrent.TimeUnit;

/**
 * Gauge that wraps a {@link Histogram} and when it's polled returns a particular percentage
 */
public class HdrHistogramGauge extends NumberGauge {
    private static final long TIMEOUT = TimeUnit.MINUTES.toMillis(1);
    private final Histogram histogram;
    private final double percentile;
    private volatile long lastCleared = System.currentTimeMillis();

    public HdrHistogramGauge(MonitorConfig monitorConfig, Histogram histogram, double percentile) {
        super(monitorConfig);
        this.histogram = histogram;
        this.percentile = percentile;

        DefaultMonitorRegistry.getInstance().register(this);
    }

    @Override
    public Long getValue() {
        long value = histogram.getValueAtPercentile(percentile);
        if (System.currentTimeMillis() - lastCleared > TIMEOUT) {
            synchronized (histogram) {
                if (System.currentTimeMillis() - lastCleared > TIMEOUT) {
                    histogram.reset();
                    lastCleared = System.currentTimeMillis();
                }
            }
        }

        return value;
    }
}
