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
import reactor.core.publisher.Flux;

public final class LeaseSpec {

  LeaseSender sender = Flux::never;
  int maxPendingRequests = 256;

  LeaseSpec() {}

  public LeaseSpec sender(LeaseSender sender) {
    this.sender = sender;
    return this;
  }

  /**
   * Setup the maximum queued requests waiting for lease to be available. The default value is 256
   *
   * @param maxPendingRequests if set to 0 the requester will terminate the request immediately if
   *     no leases is available
   */
  public LeaseSpec maxPendingRequests(int maxPendingRequests) {
    this.maxPendingRequests = maxPendingRequests;
    return this;
  }
}
