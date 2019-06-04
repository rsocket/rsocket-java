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

import io.netty.buffer.ByteBufAllocator;
import java.util.function.BiConsumer;

public class LeaseSupport {
  private final LeaseHandler leaseHandler;
  private final LeaseHandler.Requester requesterLeaseHandler;
  private final LeaseHandler.Responder responderLeaseHandler;
  private final LeaseHandler.RSocketLeases leaseSender;

  public LeaseSupport(boolean enabled, String side, ByteBufAllocator allocator) {
    leaseHandler = enabled ? new LeaseHandler(side, allocator) : null;
    requesterLeaseHandler = leaseHandler != null ? leaseHandler.requester() : null;
    responderLeaseHandler = leaseHandler != null ? leaseHandler.responder() : null;
    leaseSender = leaseHandler != null ? leaseHandler.leaseSender() : null;
  }

  public LeaseHandler.Requester requesterHandler() {
    return requesterLeaseHandler;
  }

  public LeaseHandler.Responder responderHandler() {
    return responderLeaseHandler;
  }

  public void supplyLeaseSender(BiConsumer<Leases, LeaseOptions> leaseSenderConsumer) {
    if (leaseSender != null) {
      leaseSenderConsumer.accept(leaseSender, leaseSender);
    }
  }
}
