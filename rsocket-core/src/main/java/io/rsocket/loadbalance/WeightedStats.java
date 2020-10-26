package io.rsocket.loadbalance;

/**
 * Representation of stats used by the {@link WeightedLoadbalanceStrategy}
 *
 * @since 1.1
 */
public interface WeightedStats {

  double higherQuantileLatency();

  double lowerQuantileLatency();

  int pending();

  double predictedLatency();

  double weightedAvailability();
}
