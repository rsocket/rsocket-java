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

package io.reactivesocket.spectator.internal;

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
        Assert.assertEquals(100_000, totalCount);

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
        Assert.assertEquals(200_000, totalCount);

        p90 = histogram.getValueAtPercentile(90);
        Assert.assertTrue(p90 < 100_000);
    }
}