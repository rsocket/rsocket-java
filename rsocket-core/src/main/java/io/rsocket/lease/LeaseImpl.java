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

package io.rsocket.lease;

import io.netty.buffer.ByteBuf;
import io.rsocket.frame.LeaseFlyweight;
import reactor.util.annotation.Nullable;

public final class LeaseImpl implements Lease {

  private final int allowedRequests;
  private final int ttl;
  private final long expiry;
  private final @Nullable ByteBuf metadata;

  public LeaseImpl(int allowedRequests, int ttl) {
    this(allowedRequests, ttl, null);
  }

  public LeaseImpl(int allowedRequests, int ttl, ByteBuf metadata) {
    this.allowedRequests = allowedRequests;
    this.ttl = ttl;
    expiry = System.currentTimeMillis() + ttl;
    this.metadata = metadata;
  }

  public LeaseImpl(ByteBuf leaseFrame) {
    this(
        LeaseFlyweight.numRequests(leaseFrame),
        LeaseFlyweight.ttl(leaseFrame),
        LeaseFlyweight.metadata(leaseFrame));
  }

  @Override
  public int getAllowedRequests() {
    return allowedRequests;
  }

  @Override
  public int getTtl() {
    return ttl;
  }

  @Override
  public long expiry() {
    return expiry;
  }

  @Override
  public ByteBuf getMetadata() {
    return metadata;
  }

  @Override
  public String toString() {
    return "LeaseImpl{"
        + "allowedRequests="
        + allowedRequests
        + ", ttl="
        + ttl
        + ", expiry="
        + expiry
        + '}';
  }
}
