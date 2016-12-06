package io.reactivesocket.loadbalancer.servo.internal;

import org.HdrHistogram.Histogram;
import org.junit.Assert;
import org.junit.Test;

public class SlidingWindowHistogramTest {
    @Test
    public void test() {
        SlidingWindowHistogram slidingWindowHistogram = new SlidingWindowHistogram(2);

        for (int i = 0; i < 100_000; i++) {
            slidingWindowHistogram.recordValue(i);
        }

        slidingWindowHistogram.print();
        slidingWindowHistogram.rotateHistogram();
        Histogram histogram =
            slidingWindowHistogram.aggregateHistogram();

        long totalCount = histogram.getTotalCount();
        Assert.assertTrue(totalCount == 100_000);

        long p90 = histogram.getValueAtPercentile(90);
        Assert.assertTrue(p90 < 100_000);

        for (int i = 0; i < 100_000; i++) {
            slidingWindowHistogram.recordValue(i * 10_000);
        }

        slidingWindowHistogram.print();
        slidingWindowHistogram.rotateHistogram();

        histogram =
            slidingWindowHistogram.aggregateHistogram();

        p90 = histogram.getValueAtPercentile(90);
        Assert.assertTrue(p90 >= 100_000);

        for (int i = 0; i < 100_000; i++) {
            slidingWindowHistogram.recordValue(i);
        }

        slidingWindowHistogram.print();
        slidingWindowHistogram.rotateHistogram();

        for (int i = 0; i < 100_000; i++) {
            slidingWindowHistogram.recordValue(i);
        }

        slidingWindowHistogram.print();

        histogram =
            slidingWindowHistogram.aggregateHistogram();

        totalCount = histogram.getTotalCount();
        Assert.assertTrue(totalCount == 200_000);

        p90 = histogram.getValueAtPercentile(90);
        Assert.assertTrue(p90 < 100_000);
    }
}