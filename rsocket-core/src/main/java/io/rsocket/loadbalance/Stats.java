package io.rsocket.loadbalance;

import io.rsocket.Availability;
import io.rsocket.loadbalance.stat.Ewma;
import io.rsocket.loadbalance.stat.FrugalQuantile;
import io.rsocket.loadbalance.stat.Median;
import io.rsocket.loadbalance.stat.Quantile;
import io.rsocket.util.Clock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public class Stats implements Availability {

  private static final double DEFAULT_LOWER_QUANTILE = 0.5;
  private static final double DEFAULT_HIGHER_QUANTILE = 0.8;
  private static final int INACTIVITY_FACTOR = 500;
  private static final long DEFAULT_INITIAL_INTER_ARRIVAL_TIME =
      Clock.unit().convert(1L, TimeUnit.SECONDS);

  private static final double STARTUP_PENALTY = Long.MAX_VALUE >> 12;

  private final Quantile lowerQuantile;
  private final Quantile higherQuantile;
  private final long inactivityFactor;
  private final long tau;
  private final Ewma errorPercentage;

  private long errorStamp; // last we got an error
  private long stamp; // last timestamp we sent a request
  private long stamp0; // last timestamp we sent a request or receive a response
  private long duration; // instantaneous cumulative duration
  private Median median;
  private Ewma interArrivalTime;
  private double availability = 0.0;

  private volatile int pending; // instantaneous rate
  private volatile long pendingStreams; // number of active streams
  private static final AtomicLongFieldUpdater<Stats> PENDING_STREAMS =
      AtomicLongFieldUpdater.newUpdater(Stats.class, "pendingStreams");

  public Stats() {
    this(
        new FrugalQuantile(DEFAULT_LOWER_QUANTILE),
        new FrugalQuantile(DEFAULT_HIGHER_QUANTILE),
        INACTIVITY_FACTOR);
  }

  public Stats(Quantile lowerQuantile, Quantile higherQuantile, long inactivityFactor) {
    this.lowerQuantile = lowerQuantile;
    this.higherQuantile = higherQuantile;
    this.inactivityFactor = inactivityFactor;

    long now = Clock.now();
    this.stamp = now;
    this.errorStamp = now;
    this.stamp0 = now;
    this.duration = 0L;
    this.pending = 0;
    this.median = new Median();
    this.interArrivalTime = new Ewma(1, TimeUnit.MINUTES, DEFAULT_INITIAL_INTER_ARRIVAL_TIME);
    this.errorPercentage = new Ewma(5, TimeUnit.SECONDS, 1.0);
    this.tau = Clock.unit().convert((long) (5 / Math.log(2)), TimeUnit.SECONDS);
  }

  public double errorPercentage() {
    return errorPercentage.value();
  }

  public double medianLatency() {
    return median.estimation();
  }

  public double lowerQuantileLatency() {
    return lowerQuantile.estimation();
  }

  public double higherQuantileLatency() {
    return higherQuantile.estimation();
  }

  public double interArrivalTime() {
    return interArrivalTime.value();
  }

  public int pending() {
    return pending;
  }

  public long lastTimeUsedMillis() {
    return stamp0;
  }

  @Override
  public double availability() {
    if (Clock.now() - stamp > tau) {
      recordError(1.0);
    }
    return availability * errorPercentage.value();
  }

  public synchronized double predictedLatency() {
    long now = Clock.now();
    long elapsed = Math.max(now - stamp, 1L);

    double weight;
    double prediction = median.estimation();

    if (prediction == 0.0) {
      if (pending == 0) {
        weight = 0.0; // first request
      } else {
        // subsequent requests while we don't have any history
        weight = STARTUP_PENALTY + pending;
      }
    } else if (pending == 0 && elapsed > inactivityFactor * interArrivalTime.value()) {
      // if we did't see any data for a while, we decay the prediction by inserting
      // artificial 0.0 into the median
      median.insert(0.0);
      weight = median.estimation();
    } else {
      double predicted = prediction * pending;
      double instant = instantaneous(now);

      if (predicted < instant) { // NB: (0.0 < 0.0) == false
        weight = instant / pending; // NB: pending never equal 0 here
      } else {
        // we are under the predictions
        weight = prediction;
      }
    }

    return weight;
  }

  synchronized long instantaneous(long now) {
    return duration + (now - stamp0) * pending;
  }

  void startStream() {
    PENDING_STREAMS.incrementAndGet(this);
  }

  void stopStream() {
    PENDING_STREAMS.decrementAndGet(this);
  }

  synchronized long startRequest() {
    long now = Clock.now();
    interArrivalTime.insert(now - stamp);
    duration += Math.max(0, now - stamp0) * pending;
    pending += 1;
    stamp = now;
    stamp0 = now;
    return now;
  }

  synchronized long stopRequest(long timestamp) {
    long now = Clock.now();
    duration += Math.max(0, now - stamp0) * pending - (now - timestamp);
    pending -= 1;
    stamp0 = now;
    return now;
  }

  synchronized void record(double roundTripTime) {
    median.insert(roundTripTime);
    lowerQuantile.insert(roundTripTime);
    higherQuantile.insert(roundTripTime);
  }

  synchronized void recordError(double value) {
    errorPercentage.insert(value);
    errorStamp = Clock.now();
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
        + errorPercentage.value()
        + ", pending="
        + pending
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
        + availability
        + '}';
  }

  static final class NoOpsStats extends Stats {

    static final Stats INSTANCE = new NoOpsStats();

    private NoOpsStats() {}

    @Override
    public double errorPercentage() {
      return 0.0d;
    }

    @Override
    public double medianLatency() {
      return 0.0d;
    }

    @Override
    public double lowerQuantileLatency() {
      return 0.0d;
    }

    @Override
    public double higherQuantileLatency() {
      return 0.0d;
    }

    @Override
    public double interArrivalTime() {
      return 0;
    }

    @Override
    public int pending() {
      return 0;
    }

    @Override
    public long lastTimeUsedMillis() {
      return 0;
    }

    @Override
    public double availability() {
      return 1.0d;
    }

    @Override
    public double predictedLatency() {
      return 0.0d;
    }

    @Override
    long instantaneous(long now) {
      return 0;
    }

    @Override
    void startStream() {}

    @Override
    void stopStream() {}

    @Override
    long startRequest() {
      return 0;
    }

    @Override
    long stopRequest(long timestamp) {
      return 0;
    }

    @Override
    void record(double roundTripTime) {}

    @Override
    void recordError(double value) {}

    @Override
    public String toString() {
      return "NoOpsStats{}";
    }
  }
}
