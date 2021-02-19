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

package io.rsocket.core;

import io.rsocket.lease.LeaseSender;
import io.rsocket.plugins.RequestInterceptor;
import reactor.core.publisher.Flux;
import reactor.util.concurrent.Queues;

public final class LeaseConfig {

  LeaseSender sender = Flux::never;
  RequestInterceptor statsCollector = null;
  int maxPendingRequests = 0;

  LeaseConfig() {}

  public LeaseConfig sender(LeaseSender sender) {
    this.sender = sender;
    return this;
  }

  public LeaseConfig statsCollector(RequestInterceptor requestInterceptor) {
    this.statsCollector = requestInterceptor;
    return this;
  }

  public LeaseConfig failOnNoLease() {
    this.maxPendingRequests = 0;
    return this;
  }

  public LeaseConfig deferOnNoLease() {
    return deferOnNoLease(Queues.SMALL_BUFFER_SIZE);
  }

  public LeaseConfig deferOnNoLease(int maxPendingRequests) {
    this.maxPendingRequests = maxPendingRequests;
    return this;
  }
}
