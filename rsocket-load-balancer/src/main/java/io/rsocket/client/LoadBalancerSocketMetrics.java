/*
 * Copyright 2015-2018 the original author or authors.
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

package io.rsocket.client;

import io.rsocket.Availability;

@Deprecated
/** A contract for the metrics managed by {@link LoadBalancedRSocketMono} per socket. */
public interface LoadBalancerSocketMetrics extends Availability {

  /**
   * Median value of latency as per last calculation. This is not calculated per invocation.
   *
   * @return Median latency.
   */
  double medianLatency();

  /**
   * Lower quantile of latency as per last calculation. This is not calculated per invocation.
   *
   * @return Median latency.
   */
  double lowerQuantileLatency();

  /**
   * Higher quantile value of latency as per last calculation. This is not calculated per
   * invocation.
   *
   * @return Median latency.
   */
  double higherQuantileLatency();

  /**
   * An exponentially weighted moving average value of the time between two requests.
   *
   * @return Inter arrival time.
   */
  double interArrivalTime();

  /**
   * Number of pending requests at this moment.
   *
   * @return Number of pending requests at this moment.
   */
  int pending();

  /**
   * Last time this socket was used i.e. either a request was sent or a response was received.
   *
   * @return Last time used in millis since epoch.
   */
  long lastTimeUsedMillis();
}
