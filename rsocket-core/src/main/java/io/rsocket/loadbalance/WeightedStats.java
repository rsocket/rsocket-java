package io.rsocket.loadbalance;

import io.rsocket.Availability;

/**
 * Representation of stats used by the {@link WeightedLoadbalanceStrategy}
 *
 * @since 1.1
 */
public interface WeightedStats extends Availability {

  double higherQuantileLatency();

  double lowerQuantileLatency();

  int pending();

  double predictedLatency();
}
