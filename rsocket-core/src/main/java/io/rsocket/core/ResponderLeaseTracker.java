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

package io.rsocket.core;

import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Availability;
import io.rsocket.DuplexConnection;
import io.rsocket.frame.LeaseFrameCodec;
import io.rsocket.lease.Lease;
import io.rsocket.lease.LeaseSender;
import io.rsocket.lease.MissingLeaseException;
import reactor.core.Disposable;
import reactor.core.publisher.BaseSubscriber;
import reactor.util.annotation.Nullable;

final class ResponderLeaseTracker extends BaseSubscriber<Lease>
    implements Disposable, Availability {

  final String tag;
  final ByteBufAllocator allocator;
  final DuplexConnection connection;

  @Nullable volatile MutableLease currentLease;

  ResponderLeaseTracker(String tag, DuplexConnection connection, LeaseSender leaseSender) {
    this.tag = tag;
    this.connection = connection;
    this.allocator = connection.alloc();

    leaseSender.send().subscribe(this);
  }

  @Nullable
  Throwable use() {
    final MutableLease lease = this.currentLease;
    final String tag = this.tag;

    if (lease == null) {
      return new MissingLeaseException(String.format("[%s] Lease was not issued yet", tag));
    }

    if (isExpired(lease)) {
      return new MissingLeaseException(String.format("[%s] Missing leases. Lease is expired", tag));
    }

    final int allowedRequests = lease.allowedRequests;
    final int remainingRequests = lease.remainingRequests;
    if (remainingRequests <= 0) {
      return new MissingLeaseException(
          String.format(
              "[%s] Missing leases. Issued [%s] request allowance is used", tag, allowedRequests));
    }

    lease.remainingRequests = remainingRequests - 1;

    return null;
  }

  @Override
  protected void hookOnNext(Lease lease) {
    final int allowedRequests = lease.allowedRequests();
    final int ttl = lease.timeToLiveInMillis();
    final long expireAt = lease.expireAt();

    this.currentLease = new MutableLease(allowedRequests, expireAt);
    this.connection.sendFrame(
        0, LeaseFrameCodec.encode(this.allocator, ttl, allowedRequests, lease.metadata()));
  }

  @Override
  public double availability() {
    final MutableLease lease = this.currentLease;

    if (lease == null || isExpired(lease)) {
      return 0;
    }

    return lease.remainingRequests / (double) lease.allowedRequests;
  }

  static boolean isExpired(MutableLease currentLease) {
    return System.currentTimeMillis() >= currentLease.expireAt;
  }

  static final class MutableLease {
    final int allowedRequests;
    final long expireAt;

    int remainingRequests;

    MutableLease(int allowedRequests, long expireAt) {
      this.allowedRequests = allowedRequests;
      this.expireAt = expireAt;

      this.remainingRequests = allowedRequests;
    }
  }
}
