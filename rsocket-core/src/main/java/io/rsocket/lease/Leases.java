/*
 * Copyright 2015-2019 the original author or authors.
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

package io.rsocket.lease;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import javax.annotation.Nullable;
import reactor.core.publisher.Mono;

public interface Leases {

  void sendLease(int timeToLiveMillis, int numberOfRequests, @Nullable ByteBuf metadata);

  default void sendLease(int timeToLiveMillis, int numberOfRequests) {
    sendLease(timeToLiveMillis, numberOfRequests, Unpooled.EMPTY_BUFFER);
  }

  Lease responderLease();

  void receiveLease(LeaseConsumer leaseConsumer);

  Lease requesterLease();

  Mono<Void> onClose();

  boolean isDisposed();
}
