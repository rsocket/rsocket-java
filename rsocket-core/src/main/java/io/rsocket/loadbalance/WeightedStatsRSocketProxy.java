package io.rsocket.loadbalance;

import io.rsocket.RSocket;
import io.rsocket.util.RSocketProxy;

/**
 * {@link RSocketProxy} that implements {@link WeightedStats} and delegates to an existing {@link
 * WeightedStats} instance.
 */
class WeightedStatsRSocketProxy extends RSocketProxy implements WeightedStats {

  private final WeightedStats weightedStats;

  public WeightedStatsRSocketProxy(RSocket source, WeightedStats weightedStats) {
    super(source);
    this.weightedStats = weightedStats;
  }

  @Override
  public double higherQuantileLatency() {
    return this.weightedStats.higherQuantileLatency();
  }

  @Override
  public double lowerQuantileLatency() {
    return this.weightedStats.lowerQuantileLatency();
  }

  @Override
  public int pending() {
    return this.weightedStats.pending();
  }

  @Override
  public double predictedLatency() {
    return this.weightedStats.predictedLatency();
  }

  @Override
  public double weightedAvailability() {
    return this.weightedStats.weightedAvailability();
  }

  public WeightedStats getDelegate() {
    return this.weightedStats;
  }
}
