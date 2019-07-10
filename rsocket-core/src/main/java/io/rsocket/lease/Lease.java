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
import io.rsocket.Availability;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** A contract for RSocket lease, which is sent by a request acceptor and is time bound. */
public interface Lease extends Availability {

  static Lease create(int timeToLiveMillis, int numberOfRequests, @Nullable ByteBuf metadata) {
    return LeaseImpl.create(timeToLiveMillis, numberOfRequests, metadata);
  }

  static Lease create(int timeToLiveMillis, int numberOfRequests) {
    return create(timeToLiveMillis, numberOfRequests, Unpooled.EMPTY_BUFFER);
  }

  /**
   * Number of requests allowed by this lease.
   *
   * @return The number of requests allowed by this lease.
   */
  int getAllowedRequests();

  /**
   * Initial number of requests allowed by this lease.
   *
   * @return initial number of requests allowed by this lease.
   */
  default int getStartingAllowedRequests() {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Number of milliseconds that this lease is valid from the time it is received.
   *
   * @return Number of milliseconds that this lease is valid from the time it is received.
   */
  int getTimeToLiveMillis();

  /**
   * Number of milliseconds that this lease is still valid from now.
   *
   * @param now millis since epoch
   * @return Number of milliseconds that this lease is still valid from now, or 0 if expired.
   */
  default int getRemainingTimeToLiveMillis(long now) {
    return isEmpty() ? 0 : (int) Math.max(0, expiry() - now);
  }

  /**
   * Absolute time since epoch at which this lease will expire.
   *
   * @return Absolute time since epoch at which this lease will expire.
   */
  long expiry();

  /**
   * Metadata for the lease.
   *
   * @return Metadata for the lease.
   */
  @Nonnull
  ByteBuf getMetadata();

  /**
   * Checks if the lease is expired now.
   *
   * @return {@code true} if the lease has expired.
   */
  default boolean isExpired() {
    return isExpired(System.currentTimeMillis());
  }

  /**
   * Checks if the lease is expired for the passed {@code now}.
   *
   * @param now current time in millis.
   * @return {@code true} if the lease has expired.
   */
  default boolean isExpired(long now) {
    return now > expiry();
  }

  /** Checks if the lease has not expired and there are allowed requests available */
  default boolean isValid() {
    return !isExpired() && getAllowedRequests() > 0;
  }

  /** Checks if the lease is empty(default value if no lease was received yet) */
  default boolean isEmpty() {
    return getAllowedRequests() == 0 && getTimeToLiveMillis() == 0;
  }
}
