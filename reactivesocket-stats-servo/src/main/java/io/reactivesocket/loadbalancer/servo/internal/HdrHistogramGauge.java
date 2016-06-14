package io.reactivesocket.loadbalancer.servo.internal;

import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.NumberGauge;
import org.HdrHistogram.Histogram;

/**
 * Gauge that wraps a {@link Histogram} and when it's polled returns a particular percentage
 */
public class HdrHistogramGauge extends NumberGauge {
    private final Histogram histogram;
    private final double percentile;

    public HdrHistogramGauge(MonitorConfig monitorConfig, Histogram histogram, double percentile) {
        super(monitorConfig);
        this.histogram = histogram;
        this.percentile = percentile;

        DefaultMonitorRegistry.getInstance().register(this);
    }

    @Override
    public Long getValue() {
        return histogram.getValueAtPercentile(percentile);
    }
}
