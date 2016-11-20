package io.reactivesocket.loadbalancer.servo.internal;

import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;

import java.io.PrintStream;
import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Wraps HdrHistogram to create a sliding window of n histogramQueue. Default window number is five.
 */
public class SlidingWindowHistogram {
    private static final ThreadLocal<Histogram> AGGREGATE_HISTOGRAM = ThreadLocal.withInitial(SlidingWindowHistogram::createHistogram);

    private final AtomicReference<Histogram> histogramAtomicReference;

    private final ArrayDeque<Histogram> histogramQueue;

    public SlidingWindowHistogram() {
        this(5);
    }

    public SlidingWindowHistogram(final int numOfWindows) {
        if (numOfWindows < 2) {
            throw new IllegalArgumentException("number of windows must be greater than 1");
        }
        this.histogramQueue = new ArrayDeque<>(numOfWindows - 1);
        this.histogramAtomicReference = new AtomicReference<>(createHistogram());

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
     * Records a value to the in window histogramAtomicReference
     *
     * @param value value to record
     */
    public void recordValue(long value) {
        histogramAtomicReference.get().recordValue(value);
    }

    /**
     * Slides the Histogram window. Pops a Histogram off a queue, resets it, and places the old
     * on in the queue.
     */
    public void rotateHistogram() {
        histogramAtomicReference
            .updateAndGet(old -> {
                Histogram onDeck;
                synchronized (this) {
                    onDeck = histogramQueue.poll();
                }

                if (onDeck != null) {
                    onDeck.reset();
                    synchronized (this) {
                        histogramQueue.offer(old);
                    }
                    return onDeck;
                } else {
                    return old;
                }

            });
    }

    /**
     * Aggregates the Histograms into a single histogramAtomicReference and returns it.
     *
     * @return Aggregated histogramAtomicReference
     */
    public Histogram aggregateHistogram() {
        Histogram aggregate = AGGREGATE_HISTOGRAM.get();
        aggregate.reset();

        aggregate.add(histogramAtomicReference.get());

        synchronized (this) {
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
