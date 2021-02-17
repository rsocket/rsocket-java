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
import java.time.Duration;
import reactor.util.annotation.Nullable;

/** A contract for RSocket lease, which is sent by a request acceptor and is time bound. */
public final class Lease {

  public static Lease create(
      Duration timeToLive, int numberOfRequests, @Nullable ByteBuf metadata) {
    return new Lease(timeToLive, numberOfRequests, metadata);
  }

  public static Lease create(Duration timeToLive, int numberOfRequests) {
    return create(timeToLive, numberOfRequests, Unpooled.EMPTY_BUFFER);
  }

  public static Lease unbounded() {
    return unbounded(null);
  }

  public static Lease unbounded(@Nullable ByteBuf metadata) {
    return create(Duration.ofMillis(Integer.MAX_VALUE), Integer.MAX_VALUE, metadata);
  }

  public static Lease empty() {
    return create(Duration.ZERO, 0);
  }

  final int timeToLiveMillis;
  final int allowedRequests;
  final ByteBuf metadata;
  final long expireAt;

  Lease(Duration timeToLive, int allowedRequests, @Nullable ByteBuf metadata) {
    this.allowedRequests = allowedRequests;
    this.timeToLiveMillis = (int) Math.min(timeToLive.toMillis(), Integer.MAX_VALUE);
    this.metadata = metadata == null ? Unpooled.EMPTY_BUFFER : metadata;
    this.expireAt = timeToLive.isZero() ? 0 : System.currentTimeMillis() + timeToLive.toMillis();
  }

  /**
   * Number of requests allowed by this lease.
   *
   * @return The number of requests allowed by this lease.
   */
  public int allowedRequests() {
    return allowedRequests;
  }

  /**
   * Time to live for the given lease
   *
   * @return relative duration in milliseconds
   */
  public int timeToLiveInMillis() {
    return this.timeToLiveMillis;
  }

  /**
   * Absolute time since epoch at which this lease will expire.
   *
   * @return Absolute time since epoch at which this lease will expire.
   */
  public long expireAt() {
    return expireAt;
  }

  /**
   * Metadata for the lease.
   *
   * @return Metadata for the lease.
   */
  @Nullable
  public ByteBuf metadata() {
    return metadata;
  }

  @Override
  public String toString() {
    return "Lease{"
        + "timeToLiveMillis="
        + timeToLiveMillis
        + ", allowedRequests="
        + allowedRequests
        + ", expiredAt="
        + expireAt
        + '}';
  }
}
