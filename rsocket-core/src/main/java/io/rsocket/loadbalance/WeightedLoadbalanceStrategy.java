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

import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.plugins.RequestInterceptor;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import reactor.util.annotation.Nullable;

/**
 * {@link LoadbalanceStrategy} that assigns a weight to each {@code RSocket} based on usage
 * statistics, and uses this weight to select the {@code RSocket} to use.
 *
 * @since 1.1
 */
public class WeightedLoadbalanceStrategy implements ClientLoadbalanceStrategy {

  private static final double EXP_FACTOR = 4.0;

  private static final int EFFORT = 5;

  final int effort;
  final SplittableRandom splittableRandom;
  final Function<RSocket, WeightedStats> weightedStatsResolver;

  public WeightedLoadbalanceStrategy() {
    this(new DefaultWeightedStatsResolver());
  }

  public WeightedLoadbalanceStrategy(Function<RSocket, WeightedStats> weightedStatsResolver) {
    this(EFFORT, weightedStatsResolver);
  }

  public WeightedLoadbalanceStrategy(
      int effort, Function<RSocket, WeightedStats> weightedStatsResolver) {
    this(effort, new SplittableRandom(System.nanoTime()), weightedStatsResolver);
  }

  public WeightedLoadbalanceStrategy(
      int effort,
      SplittableRandom splittableRandom,
      Function<RSocket, WeightedStats> weightedStatsResolver) {
    this.effort = effort;
    this.splittableRandom = splittableRandom;
    this.weightedStatsResolver = weightedStatsResolver;
  }

  @Override
  public void initialize(RSocketConnector connector) {
    final Function<RSocket, WeightedStats> resolver = weightedStatsResolver;
    if (resolver instanceof DefaultWeightedStatsResolver) {
      ((DefaultWeightedStatsResolver) resolver).init(connector);
    }
  }

  @Override
  public RSocket select(List<RSocket> sockets) {
    final int effort = this.effort;
    final int size = sockets.size();

    RSocket weightedRSocket;
    final Function<RSocket, WeightedStats> weightedStatsResolver = this.weightedStatsResolver;
    switch (size) {
      case 1:
        weightedRSocket = sockets.get(0);
        break;
      case 2:
        {
          RSocket rsc1 = sockets.get(0);
          RSocket rsc2 = sockets.get(1);

          double w1 = algorithmicWeight(rsc1, weightedStatsResolver.apply(rsc1));
          double w2 = algorithmicWeight(rsc2, weightedStatsResolver.apply(rsc2));
          if (w1 < w2) {
            weightedRSocket = rsc2;
          } else {
            weightedRSocket = rsc1;
          }
        }
        break;
      default:
        {
          RSocket rsc1 = null;
          RSocket rsc2 = null;

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

          if (rsc1 != null & rsc2 != null) {
            double w1 = algorithmicWeight(rsc1, weightedStatsResolver.apply(rsc1));
            double w2 = algorithmicWeight(rsc2, weightedStatsResolver.apply(rsc2));

            if (w1 < w2) {
              weightedRSocket = rsc2;
            } else {
              weightedRSocket = rsc1;
            }
          } else if (rsc1 != null) {
            weightedRSocket = rsc1;
          } else {
            weightedRSocket = rsc2;
          }
        }
    }

    return weightedRSocket;
  }

  private static double algorithmicWeight(
      RSocket rSocket, @Nullable final WeightedStats weightedStats) {
    if (weightedStats == null || rSocket.isDisposed() || rSocket.availability() == 0.0) {
      return 0.0;
    }
    final int pending = weightedStats.pending();

    double latency = weightedStats.predictedLatency();

    final double low = weightedStats.lowerQuantileLatency();
    final double high =
        Math.max(
            weightedStats.higherQuantileLatency(),
            low * 1.001); // ensure higherQuantile > lowerQuantile + .1%
    final double bandWidth = Math.max(high - low, 1);

    if (latency < low) {
      latency /= calculateFactor(low, latency, bandWidth);
    } else if (latency > high) {
      latency *= calculateFactor(latency, high, bandWidth);
    }

    return rSocket.availability() / (1.0d + latency * (pending + 1));
  }

  private static double calculateFactor(final double u, final double l, final double bandWidth) {
    final double alpha = (u - l) / bandWidth;
    return Math.pow(1 + alpha, EXP_FACTOR);
  }

  static class DefaultWeightedStatsResolver implements Function<RSocket, WeightedStats> {

    final ConcurrentMap<RSocket, WeightedStatsRequestInterceptor> rsocketsInterceptors =
        new ConcurrentHashMap<>();

    @Override
    public WeightedStats apply(RSocket rSocket) {
      return rsocketsInterceptors.get(rSocket);
    }

    void init(RSocketConnector connector) {
      connector.interceptors(
          ir ->
              ir.forRequester(
                  (Function<RSocket, ? extends RequestInterceptor>)
                      rSocket -> {
                        final WeightedStatsRequestInterceptor interceptor =
                            new WeightedStatsRequestInterceptor() {
                              @Override
                              public void dispose() {
                                rsocketsInterceptors.remove(rSocket);
                              }
                            };
                        rsocketsInterceptors.put(rSocket, interceptor);

                        return interceptor;
                      }));
    }
  }
}
