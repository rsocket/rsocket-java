/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket.loadbalancer.servo.internal;

import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.tag.Tag;
import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Captures a HdrHistogram and sends it to pre-defined Server Counters.
 * The buckets are min, max, 50%, 90%, 99%, 99.9%, and 99.99%
 */
public class HdrHistogramServoTimer {
    private final Histogram histogram = new ConcurrentHistogram(TimeUnit.MINUTES.toNanos(1), 2);

    private HdrHistogramMinGauge min;

    private HdrHistogramMaxGauge max;

    private HdrHistogramGauge p50;

    private HdrHistogramGauge p90;

    private HdrHistogramGauge p99;

    private HdrHistogramGauge p99_9;

    private HdrHistogramGauge p99_99;

    private HdrHistogramServoTimer(String label) {
        histogram.setAutoResize(true);

        min = new HdrHistogramMinGauge(MonitorConfig.builder(label + "_min").build(), histogram);
        max = new HdrHistogramMaxGauge(MonitorConfig.builder(label + "_max").build(), histogram);

        p50 = new HdrHistogramGauge(MonitorConfig.builder(label + "_p50").build(), histogram, 50);
        p90 = new HdrHistogramGauge(MonitorConfig.builder(label + "_p90").build(), histogram, 90);
        p99 = new HdrHistogramGauge(MonitorConfig.builder(label + "_p99").build(), histogram, 99);
        p99_9 = new HdrHistogramGauge(MonitorConfig.builder(label + "_p99_9").build(), histogram, 99.9);
        p99_99 = new HdrHistogramGauge(MonitorConfig.builder(label + "_p99_99").build(), histogram, 99.99);
    }


    private HdrHistogramServoTimer(String label, List<Tag> tags) {
        histogram.setAutoResize(true);


        min = new HdrHistogramMinGauge(MonitorConfig.builder(label + "_min").withTags(tags).build(), histogram);
        max = new HdrHistogramMaxGauge(MonitorConfig.builder(label + "_max").withTags(tags).build(), histogram);

        p50 = new HdrHistogramGauge(MonitorConfig.builder(label + "_p50").withTags(tags).build(), histogram, 50);
        p90 = new HdrHistogramGauge(MonitorConfig.builder(label + "_p90").withTags(tags).build(), histogram, 90);
        p99 = new HdrHistogramGauge(MonitorConfig.builder(label + "_p99").withTags(tags).build(), histogram, 99);
        p99_9 = new HdrHistogramGauge(MonitorConfig.builder(label + "_p99_9").withTags(tags).build(), histogram, 99.9);
        p99_99 = new HdrHistogramGauge(MonitorConfig.builder(label + "_p99_99").withTags(tags).build(), histogram, 99.99);
    }

    public static HdrHistogramServoTimer newInstance(String label) {
        return new HdrHistogramServoTimer(label);
    }

    public static HdrHistogramServoTimer newInstance(String label, Tag... tags) {
        return newInstance(label, Arrays.asList(tags));
    }

    public static HdrHistogramServoTimer newInstance(String label, List<Tag> tags) {
        return new HdrHistogramServoTimer(label, tags);
    }

    /**
     * Records a value for to the histogram and updates the Servo counter buckets
     * @param value the value to update
     */
    public void record(long value) {
        histogram.recordValue(value);
    }

    public Long getMin() {
        return min.getValue();
    }

    public Long getMax() {
        return max.getValue();
    }

    public Long getP50() {
        return p50.getValue();
    }

    public Long getP90() {
        return p90.getValue();
    }

    public Long getP99() {
        return p99.getValue();
    }

    public Long getP99_9() {
        return p99_9.getValue();
    }

    public Long getP99_99() {
        return p99_99.getValue();
    }
}