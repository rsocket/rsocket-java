package io.rsocket.loadbalance;

import io.rsocket.util.Clock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * The base implementation of the {@link WeightedStats} interface
 *
 * @since 1.1
 */
public class BaseWeightedStats implements WeightedStats {

  private static final double DEFAULT_LOWER_QUANTILE = 0.5;
  private static final double DEFAULT_HIGHER_QUANTILE = 0.8;
  private static final int INACTIVITY_FACTOR = 500;
  private static final long DEFAULT_INITIAL_INTER_ARRIVAL_TIME =
      Clock.unit().convert(1L, TimeUnit.SECONDS);

  private static final double STARTUP_PENALTY = Long.MAX_VALUE >> 12;

  private final Quantile lowerQuantile;
  private final Quantile higherQuantile;
  private final Ewma availabilityPercentage;
  private final Median median;
  private final Ewma interArrivalTime;

  private final long tau;
  private final long inactivityFactor;

  private long errorStamp; // last we got an error
  private long stamp; // last timestamp we sent a request
  private long stamp0; // last timestamp we sent a request or receive a response
  private long duration; // instantaneous cumulative duration

  private volatile int pendingRequests; // instantaneous rate
  private static final AtomicIntegerFieldUpdater<BaseWeightedStats> PENDING_REQUESTS =
      AtomicIntegerFieldUpdater.newUpdater(BaseWeightedStats.class, "pendingRequests");
  private volatile int pendingStreams; // number of active streams
  private static final AtomicIntegerFieldUpdater<BaseWeightedStats> PENDING_STREAMS =
      AtomicIntegerFieldUpdater.newUpdater(BaseWeightedStats.class, "pendingStreams");

  protected BaseWeightedStats() {
    this(
        new FrugalQuantile(DEFAULT_LOWER_QUANTILE),
        new FrugalQuantile(DEFAULT_HIGHER_QUANTILE),
        INACTIVITY_FACTOR);
  }

  private BaseWeightedStats(
      Quantile lowerQuantile, Quantile higherQuantile, long inactivityFactor) {
    this.lowerQuantile = lowerQuantile;
    this.higherQuantile = higherQuantile;
    this.inactivityFactor = inactivityFactor;

    long now = Clock.now();
    this.stamp = now;
    this.errorStamp = now;
    this.stamp0 = now;
    this.duration = 0L;
    this.pendingRequests = 0;
    this.median = new Median();
    this.interArrivalTime = new Ewma(1, TimeUnit.MINUTES, DEFAULT_INITIAL_INTER_ARRIVAL_TIME);
    this.availabilityPercentage = new Ewma(5, TimeUnit.SECONDS, 1.0);
    this.tau = Clock.unit().convert((long) (5 / Math.log(2)), TimeUnit.SECONDS);
  }

  @Override
  public double lowerQuantileLatency() {
    return lowerQuantile.estimation();
  }

  @Override
  public double higherQuantileLatency() {
    return higherQuantile.estimation();
  }

  @Override
  public int pending() {
    return pendingRequests + pendingStreams;
  }

  @Override
  public double weightedAvailability() {
    if (Clock.now() - stamp > tau) {
      updateAvailability(1.0);
    }
    return availabilityPercentage.value();
  }

  @Override
  public double predictedLatency() {
    final long now = Clock.now();
    final long elapsed;

    synchronized (this) {
      elapsed = Math.max(now - stamp, 1L);
    }

    final double latency;
    final double prediction = median.estimation();

    final int pending = this.pending();
    if (prediction == 0.0) {
      if (pending == 0) {
        latency = 0.0; // first request
      } else {
        // subsequent requests while we don't have any history
        latency = STARTUP_PENALTY + pending;
      }
    } else if (pending == 0 && elapsed > inactivityFactor * interArrivalTime.value()) {
      // if we did't see any data for a while, we decay the prediction by inserting
      // artificial 0.0 into the median
      median.insert(0.0);
      latency = median.estimation();
    } else {
      final double predicted = prediction * pending;
      final double instant = instantaneous(now, pending);

      if (predicted < instant) { // NB: (0.0 < 0.0) == false
        latency = instant / pending; // NB: pending never equal 0 here
      } else {
        // we are under the predictions
        latency = prediction;
      }
    }

    return latency;
  }

  long instantaneous(long now, int pending) {
    return duration + (now - stamp0) * pending;
  }

  void startStream() {
    PENDING_STREAMS.incrementAndGet(this);
  }

  void stopStream() {
    PENDING_STREAMS.decrementAndGet(this);
  }

  synchronized long startRequest() {
    final long now = Clock.now();
    final int pendingRequests = this.pendingRequests;

    interArrivalTime.insert(now - stamp);
    duration += Math.max(0, now - stamp0) * pendingRequests;
    PENDING_REQUESTS.lazySet(this, pendingRequests + 1);
    stamp = now;
    stamp0 = now;

    return now;
  }

  synchronized long stopRequest(long timestamp) {
    final long now = Clock.now();
    final int pendingRequests = this.pendingRequests;

    duration += Math.max(0, now - stamp0) * pendingRequests - (now - timestamp);
    PENDING_REQUESTS.lazySet(this, pendingRequests - 1);
    stamp0 = now;

    return now;
  }

  synchronized void record(double roundTripTime) {
    median.insert(roundTripTime);
    lowerQuantile.insert(roundTripTime);
    higherQuantile.insert(roundTripTime);
  }

  void updateAvailability(double value) {
    availabilityPercentage.insert(value);
    if (value == 0.0d) {
      synchronized (this) {
        errorStamp = Clock.now();
      }
    }
  }

  @Override
  public String toString() {
    return "Stats{"
        + "lowerQuantile="
        + lowerQuantile.estimation()
        + ", higherQuantile="
        + higherQuantile.estimation()
        + ", inactivityFactor="
        + inactivityFactor
        + ", tau="
        + tau
        + ", errorPercentage="
        + availabilityPercentage.value()
        + ", pending="
        + pendingRequests
        + ", errorStamp="
        + errorStamp
        + ", stamp="
        + stamp
        + ", stamp0="
        + stamp0
        + ", duration="
        + duration
        + ", median="
        + median.estimation()
        + ", interArrivalTime="
        + interArrivalTime.value()
        + ", pendingStreams="
        + pendingStreams
        + ", availability="
        + availabilityPercentage.value()
        + '}';
  }
}
