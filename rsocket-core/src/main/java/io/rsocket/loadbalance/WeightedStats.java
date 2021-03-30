/*
 * Copyright 2015-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rsocket.loadbalance;

import io.rsocket.RSocket;

/**
 * Contract to expose the stats required in {@link WeightedLoadbalanceStrategy} to calculate an
 * algorithmic weight for an {@code RSocket}. The weight helps to select an {@code RSocket} for
 * load-balancing.
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
   * Create a proxy for the given {@code RSocket} that attaches the stats contained in this instance
   * and exposes them as {@link WeightedStats}.
   *
   * @param rsocket the RSocket to wrap
   * @return the wrapped RSocket
   * @since 1.1.1
   */
  default RSocket wrap(RSocket rsocket) {
    return new WeightedStatsRSocketProxy(rsocket, this);
  }
}
