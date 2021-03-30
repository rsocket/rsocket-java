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

import io.rsocket.core.RSocketConnector;
import io.rsocket.plugins.InterceptorRegistry;

/**
 * A {@link LoadbalanceStrategy} with an interest in configuring the {@link RSocketConnector} for
 * connecting to load-balance targets in order to hook into request lifecycle and track usage
 * statistics.
 *
 * <p>Currently this callback interface is supported for strategies configured in {@link
 * LoadbalanceRSocketClient}.
 *
 * @since 1.1
 */
public interface ClientLoadbalanceStrategy extends LoadbalanceStrategy {

  /**
   * Initialize the connector, for example using the {@link InterceptorRegistry}, to intercept
   * requests.
   *
   * @param connector the connector to configure
   */
  void initialize(RSocketConnector connector);
}
