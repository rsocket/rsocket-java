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
import io.rsocket.core.RSocketConnector;
import io.rsocket.plugins.RequestInterceptor;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import reactor.util.annotation.Nullable;

/**
 * {@link LoadbalanceStrategy} that assigns a weight to each {@code RSocket} based on {@link
 * RSocket#availability() availability} and usage statistics. The weight is used to decide which
 * {@code RSocket} to select.
 *
 * <p>Use {@link #create()} or a {@link #builder() Builder} to create an instance.
 *
 * @since 1.1
 * @see <a href="https://www.youtube.com/watch?v=6NdxUY1La2I">Predictive Load-Balancing: Unfair but
 *     Faster & more Robust</a>
 * @see WeightedStatsRequestInterceptor
 */
public class WeightedLoadbalanceStrategy implements ClientLoadbalanceStrategy {

  private static final double EXP_FACTOR = 4.0;

  final int maxPairSelectionAttempts;
  final Function<RSocket, WeightedStats> weightedStatsResolver;

  private WeightedLoadbalanceStrategy(
      int numberOfAttempts, @Nullable Function<RSocket, WeightedStats> resolver) {
    this.maxPairSelectionAttempts = numberOfAttempts;
    this.weightedStatsResolver = (resolver != null ? resolver : new DefaultWeightedStatsResolver());
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

          for (int i = 0; i < this.maxPairSelectionAttempts; i++) {
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
    if (weightedStats == null) {
      return 1.0;
    }
    if (rSocket.isDisposed() || rSocket.availability() == 0.0) {
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

    return (rSocket.availability() * weightedStats.weightedAvailability())
        / (1.0d + latency * (pending + 1));
  }

  private static double calculateFactor(final double u, final double l, final double bandWidth) {
    final double alpha = (u - l) / bandWidth;
    return Math.pow(1 + alpha, EXP_FACTOR);
  }

  /**
   * Create an instance of {@link WeightedLoadbalanceStrategy} with default settings, which include
   * round-robin load-balancing and 5 {@link #maxPairSelectionAttempts}.
   */
  public static WeightedLoadbalanceStrategy create() {
    return new Builder().build();
  }

  /** Return a builder to create a {@link WeightedLoadbalanceStrategy} with. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link WeightedLoadbalanceStrategy}. */
  public static class Builder {

    private int maxPairSelectionAttempts = 5;

    @Nullable private Function<RSocket, WeightedStats> weightedStatsResolver;

    private Builder() {}

    /**
     * How many times to try to randomly select a pair of RSocket connections with non-zero
     * availability. This is applicable when there are more than two connections in the pool. If the
     * number of attempts is exceeded, the last selected pair is used.
     *
     * <p>By default this is set to 5.
     *
     * @param numberOfAttempts the iteration count
     */
    public Builder maxPairSelectionAttempts(int numberOfAttempts) {
      this.maxPairSelectionAttempts = numberOfAttempts;
      return this;
    }

    /**
     * Configure how the created {@link WeightedLoadbalanceStrategy} should find the stats for a
     * given RSocket.
     *
     * <p>By default this resolver is not set.
     *
     * <p>When {@code WeightedLoadbalanceStrategy} is used through the {@link
     * LoadbalanceRSocketClient}, the resolver does not need to be set because a {@link
     * WeightedStatsRequestInterceptor} is automatically installed through the {@link
     * ClientLoadbalanceStrategy} callback. If this strategy is used in any other context however, a
     * resolver here must be provided.
     *
     * @param resolver to find the stats for an RSocket with
     */
    public Builder weightedStatsResolver(Function<RSocket, WeightedStats> resolver) {
      this.weightedStatsResolver = resolver;
      return this;
    }

    /** Build the {@code WeightedLoadbalanceStrategy} instance. */
    public WeightedLoadbalanceStrategy build() {
      return new WeightedLoadbalanceStrategy(
          this.maxPairSelectionAttempts, this.weightedStatsResolver);
    }
  }

  private static class DefaultWeightedStatsResolver implements Function<RSocket, WeightedStats> {

    final Map<RSocket, WeightedStats> statsMap = new ConcurrentHashMap<>();

    @Override
    public WeightedStats apply(RSocket rSocket) {
      return statsMap.get(rSocket);
    }

    void init(RSocketConnector connector) {
      connector.interceptors(
          registry ->
              registry.forRequestsInRequester(
                  (Function<RSocket, ? extends RequestInterceptor>)
                      rSocket -> {
                        final WeightedStatsRequestInterceptor interceptor =
                            new WeightedStatsRequestInterceptor() {
                              @Override
                              public void dispose() {
                                statsMap.remove(rSocket);
                              }
                            };
                        statsMap.put(rSocket, interceptor);

                        return interceptor;
                      }));
    }
  }
}
