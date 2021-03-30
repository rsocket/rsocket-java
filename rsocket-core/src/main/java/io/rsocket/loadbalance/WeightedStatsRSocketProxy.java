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
import io.rsocket.util.RSocketProxy;

/**
 * Package private {@code RSocketProxy} used from {@link WeightedStats#wrap(RSocket)} to attach a
 * {@link WeightedStats} instance to an {@code RSocket}.
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
