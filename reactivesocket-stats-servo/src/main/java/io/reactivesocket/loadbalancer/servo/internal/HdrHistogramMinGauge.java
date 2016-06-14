package io.reactivesocket.loadbalancer.servo.internal;

import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.NumberGauge;
import org.HdrHistogram.Histogram;

/**
 * Gauge that wraps a {@link Histogram} and when its polled returns it's min
 */
public class HdrHistogramMinGauge extends NumberGauge {
    private final Histogram histogram;

    public HdrHistogramMinGauge(MonitorConfig monitorConfig, Histogram histogram) {
        super(monitorConfig);
        this.histogram = histogram;

        DefaultMonitorRegistry.getInstance().register(this);
    }

    @Override
    public Long getValue() {
        return histogram.getMinValue();
    }
}
