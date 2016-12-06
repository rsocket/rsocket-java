package io.reactivesocket.loadbalancer.servo.internal;

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

    private final Object LOCK = new Object();

    public SlidingWindowHistogram() {
        this(5);
    }

    public SlidingWindowHistogram(final int numOfWindows) {
        if (numOfWindows < 2) {
            throw new IllegalArgumentException("number of windows must be greater than 1");
        }
        this.histogramQueue = new ArrayDeque<>(numOfWindows - 1);
        this.liveHistogram = createHistogram();

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
        synchronized (LOCK) {
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

        synchronized (LOCK) {
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
