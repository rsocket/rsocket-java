package io.rsocket.loadbalance;

import io.rsocket.RSocket;

/**
 * Representation of stats used by the {@link WeightedLoadbalanceStrategy}.
 *
 * @since 1.1
 */
public interface WeightedStats {

  double higherQuantileLatency();

  double lowerQuantileLatency();

  int pending();

  double predictedLatency();

  double weightedAvailability();

  /**
   * Wraps an RSocket with a proxy that implements WeightedStats.
   *
   * @param rsocket the RSocket to proxy.
   * @return the wrapped RSocket.
   * @since 1.1.1
   */
  default RSocket wrap(RSocket rsocket) {
    return new WeightedStatsRSocketProxy(rsocket, this);
  }
}
