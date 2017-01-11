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

import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;

import java.io.PrintStream;
import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;

/**
 * Wraps HdrHistogram to create a sliding window of n histogramQueue. Default window number is five.
 */
public class SlidingWindowHistogram {

    private volatile Histogram liveHistogram;

    private final ArrayDeque<Histogram> histogramQueue;

    private final Object lock = new Object();

    public SlidingWindowHistogram() {
        this(5);
    }

    public SlidingWindowHistogram(final int numOfWindows) {
        if (numOfWindows < 2) {
            throw new IllegalArgumentException("number of windows must be greater than 1");
        }
        histogramQueue = new ArrayDeque<>(numOfWindows - 1);
        liveHistogram = createHistogram();

        for (int i = 0; i < numOfWindows - 1; i++) {
            histogramQueue.offer(createHistogram());
        }
    }

    private static Histogram createHistogram() {
        ConcurrentHistogram histogram = new ConcurrentHistogram(TimeUnit.MINUTES.toNanos(1), 2);
        histogram.setAutoResize(true);
        return histogram;
    }

    /**
     * Records a value to the in window liveHistogram
     *
     * @param value value to record
     */
    public void recordValue(long value) {
        liveHistogram.recordValue(value);
    }

    /**
     * Slides the Histogram window. Pops a Histogram off a queue, resets it, and places the old
     * on in the queue.
     */
    public void rotateHistogram() {
        synchronized (lock) {
            Histogram onDeck = histogramQueue.poll();
            if (onDeck != null) {
                onDeck.reset();
                Histogram old = liveHistogram;
                liveHistogram = onDeck;
                histogramQueue.offer(old);
            }
        }
    }

    /**
     * Aggregates the Histograms into a single Histogram and returns it.
     *
     * @return Aggregated liveHistogram
     */
    public Histogram aggregateHistogram() {
        Histogram aggregate = createHistogram();

        synchronized (lock) {
            aggregate.add(liveHistogram);
            histogramQueue
                .forEach(aggregate::add);
        }

        return aggregate;
    }

    /**
     * Prints HdrHistogram to System.out
     */
    public void print() {
        print(System.out);
    }

    public void print(PrintStream printStream) {
        aggregateHistogram().outputPercentileDistribution(printStream, 1000.0);
    }

}
