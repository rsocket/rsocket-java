package io.reactivesocket.loadbalancer.servo.internal;

import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.NumberGauge;
import org.HdrHistogram.Histogram;

/**
 * Gauge that wraps a {@link Histogram} and when its polled returns it's max
 */
public class HdrHistogramMaxGauge extends NumberGauge {
    private final Histogram histogram;

    public HdrHistogramMaxGauge(MonitorConfig monitorConfig, Histogram histogram) {
        super(monitorConfig);
        this.histogram = histogram;

        DefaultMonitorRegistry.getInstance().register(this);
    }

    @Override
    public Long getValue() {
        return histogram.getMaxValue();
    }
}
