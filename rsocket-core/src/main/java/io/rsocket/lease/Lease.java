/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.lease;

import java.nio.ByteBuffer;
import javax.annotation.Nullable;

/** A contract for RSocket lease, which is sent by a request acceptor and is time bound. */
public interface Lease {

  /**
   * Number of requests allowed by this lease.
   *
   * @return The number of requests allowed by this lease.
   */
  int getAllowedRequests();

  /**
   * Number of seconds that this lease is valid from the time it is received.
   *
   * @return Number of seconds that this lease is valid from the time it is received.
   */
  int getTtl();

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
  @Nullable
  ByteBuffer getMetadata();

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
   * @return {@code true} if the lease has expired.
   */
  default boolean isExpired(long now) {
    return now > expiry();
  }
}
