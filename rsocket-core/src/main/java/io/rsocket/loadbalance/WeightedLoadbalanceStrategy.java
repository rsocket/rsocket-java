/*
 * Copyright 2015-2020 the original author or authors.
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

import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import reactor.util.annotation.Nullable;

public class WeightedLoadbalanceStrategy implements LoadbalanceStrategy {

  private static final double EXP_FACTOR = 4.0;

  private static final int EFFORT = 5;

  final SplittableRandom splittableRandom;
  final int effort;
  final Supplier<Stats> statsSupplier;

  public WeightedLoadbalanceStrategy() {
    this(EFFORT);
  }

  public WeightedLoadbalanceStrategy(int effort) {
    this(effort, new SplittableRandom(System.nanoTime()));
  }

  public WeightedLoadbalanceStrategy(int effort, SplittableRandom splittableRandom) {
    this(effort, splittableRandom, Stats::create);
  }

  public WeightedLoadbalanceStrategy(
      int effort, SplittableRandom splittableRandom, Supplier<Stats> statsSupplier) {
    this.splittableRandom = splittableRandom;
    this.effort = effort;
    this.statsSupplier = statsSupplier;
  }

  @Override
  public Supplier<Stats> statsSupplier() {
    return this.statsSupplier;
  }

  @Override
  public WeightedRSocket select(List<WeightedRSocket> sockets) {
    final int effort = this.effort;
    final int size = sockets.size();

    WeightedRSocket weightedRSocket;
    switch (size) {
      case 1:
        weightedRSocket = sockets.get(0);
        break;
      case 2:
        {
          WeightedRSocket rsc1 = sockets.get(0);
          WeightedRSocket rsc2 = sockets.get(1);

          double w1 = algorithmicWeight(rsc1);
          double w2 = algorithmicWeight(rsc2);
          if (w1 < w2) {
            weightedRSocket = rsc2;
          } else {
            weightedRSocket = rsc1;
          }
        }
        break;
      default:
        {
          WeightedRSocket rsc1 = null;
          WeightedRSocket rsc2 = null;

          for (int i = 0; i < effort; i++) {
            int i1 = ThreadLocalRandom.current().nextInt(size);
            int i2 = ThreadLocalRandom.current().nextInt(size - 1);

            if (i2 >= i1) {
              i2++;
            }
            rsc1 = sockets.get(i1);
            rsc2 = sockets.get(i2);
            if (rsc1.availability() > 0.0 && rsc2.availability() > 0.0) {
              break;
            }
          }

          double w1 = algorithmicWeight(rsc1);
          double w2 = algorithmicWeight(rsc2);
          if (w1 < w2) {
            weightedRSocket = rsc2;
          } else {
            weightedRSocket = rsc1;
          }
        }
    }

    return weightedRSocket;
  }

  private static double algorithmicWeight(@Nullable final WeightedRSocket weightedRSocket) {
    if (weightedRSocket == null
        || weightedRSocket.isDisposed()
        || weightedRSocket.availability() == 0.0) {
      return 0.0;
    }
    final Stats stats = weightedRSocket.stats();
    final int pending = stats.pending();
    double latency = stats.predictedLatency();

    final double low = stats.lowerQuantileLatency();
    final double high =
        Math.max(
            stats.higherQuantileLatency(),
            low * 1.001); // ensure higherQuantile > lowerQuantile + .1%
    final double bandWidth = Math.max(high - low, 1);

    if (latency < low) {
      latency /= calculateFactor(low, latency, bandWidth);
    } else if (latency > high) {
      latency *= calculateFactor(latency, high, bandWidth);
    }

    return weightedRSocket.availability() * 1.0 / (1.0 + latency * (pending + 1));
  }

  private static double calculateFactor(final double u, final double l, final double bandWidth) {
    final double alpha = (u - l) / bandWidth;
    return Math.pow(1 + alpha, EXP_FACTOR);
  }
}
