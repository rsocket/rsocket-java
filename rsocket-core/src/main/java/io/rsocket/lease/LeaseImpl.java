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
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class LeaseImpl implements Lease {
  private final int timeToLiveMillis;
  private final AtomicInteger allowedRequests;
  private final int startingAllowedRequests;
  private final ByteBuf metadata;
  private final long expiry;

  static LeaseImpl create(int timeToLiveMillis, int numberOfRequests, @Nullable ByteBuf metadata) {
    assertLease(timeToLiveMillis, numberOfRequests);
    return new LeaseImpl(timeToLiveMillis, numberOfRequests, metadata);
  }

  static LeaseImpl empty() {
    return new LeaseImpl(0, 0, null);
  }

  private LeaseImpl(int timeToLiveMillis, int allowedRequests, @Nullable ByteBuf metadata) {
    this.allowedRequests = new AtomicInteger(allowedRequests);
    this.startingAllowedRequests = allowedRequests;
    this.timeToLiveMillis = timeToLiveMillis;
    this.metadata = metadata == null ? Unpooled.EMPTY_BUFFER : metadata;
    this.expiry = timeToLiveMillis == 0 ? 0 : now() + timeToLiveMillis;
  }

  public int getTimeToLiveMillis() {
    return timeToLiveMillis;
  }

  @Override
  public int getAllowedRequests() {
    return Math.max(0, allowedRequests.get());
  }

  @Override
  public int getStartingAllowedRequests() {
    return startingAllowedRequests;
  }

  @Nonnull
  @Override
  public ByteBuf getMetadata() {
    return metadata;
  }

  @Override
  public long expiry() {
    return expiry;
  }

  @Override
  public boolean isValid() {
    return !isEmpty() && getAllowedRequests() > 0 && !isExpired();
  }

  /**
   * try use 1 allowed request of Lease
   *
   * @return true if used successfully, false if Lease is expired or no allowed requests available
   */
  public boolean use() {
    if (isExpired()) {
      return false;
    }
    int remaining =
        allowedRequests.accumulateAndGet(1, (cur, update) -> Math.max(-1, cur - update));
    return remaining >= 0;
  }

  @Override
  public double availability() {
    return isValid() ? getAllowedRequests() / (double) getStartingAllowedRequests() : 0.0;
  }

  @Override
  public String toString() {
    long now = now();
    return "LeaseImpl{"
        + "timeToLiveMillis="
        + timeToLiveMillis
        + ", allowedRequests="
        + getAllowedRequests()
        + ", startingAllowedRequests="
        + startingAllowedRequests
        + ", expired="
        + isExpired(now)
        + ", remainingTimeToLiveMillis="
        + getRemainingTimeToLiveMillis(now)
        + '}';
  }

  private static long now() {
    return System.currentTimeMillis();
  }

  private static void assertLease(int timeToLiveMillis, int numberOfRequests) {
    if (numberOfRequests <= 0) {
      throw new IllegalArgumentException("Number of requests must be positive");
    }
    if (timeToLiveMillis <= 0) {
      throw new IllegalArgumentException("Time-to-live must be positive");
    }
  }
}
