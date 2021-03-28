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

import io.netty.buffer.ByteBuf;
import io.rsocket.Availability;
import io.rsocket.frame.LeaseFrameCodec;
import io.rsocket.lease.Lease;
import io.rsocket.lease.MissingLeaseException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;

final class RequesterLeaseTracker implements Availability {

  final String tag;
  final int maximumAllowedAwaitingPermitHandlersNumber;
  final Queue<LeasePermitHandler> awaitingPermitHandlersQueue;

  Lease currentLease = null;
  int availableRequests;

  boolean isDisposed;
  Throwable t;

  RequesterLeaseTracker(String tag, int maximumAllowedAwaitingPermitHandlersNumber) {
    this.tag = tag;
    this.maximumAllowedAwaitingPermitHandlersNumber = maximumAllowedAwaitingPermitHandlersNumber;
    this.awaitingPermitHandlersQueue = new ArrayDeque<>();
  }

  synchronized void issue(LeasePermitHandler leasePermitHandler) {
    if (this.isDisposed) {
      leasePermitHandler.handlePermitError(this.t);
      return;
    }

    final int availableRequests = this.availableRequests;
    final Lease l = this.currentLease;
    final boolean leaseReceived = l != null;
    final boolean isExpired = leaseReceived && isExpired(l);

    if (leaseReceived && availableRequests > 0 && !isExpired) {
      if (leasePermitHandler.handlePermit()) {
        this.availableRequests = availableRequests - 1;
      }
    } else {
      final Queue<LeasePermitHandler> queue = this.awaitingPermitHandlersQueue;
      if (this.maximumAllowedAwaitingPermitHandlersNumber > queue.size()) {
        queue.offer(leasePermitHandler);
      } else {
        final String tag = this.tag;
        final String message;
        if (!leaseReceived) {
          message = String.format("[%s] Lease was not received yet", tag);
        } else if (isExpired) {
          message = String.format("[%s] Missing leases. Lease is expired", tag);
        } else {
          message =
              String.format(
                  "[%s] Missing leases. Issued [%s] request allowance is used",
                  tag, availableRequests);
        }

        final Throwable t = new MissingLeaseException(message);
        leasePermitHandler.handlePermitError(t);
      }
    }
  }

  void handleLeaseFrame(ByteBuf leaseFrame) {
    final int numberOfRequests = LeaseFrameCodec.numRequests(leaseFrame);
    final int timeToLiveMillis = LeaseFrameCodec.ttl(leaseFrame);
    final ByteBuf metadata = LeaseFrameCodec.metadata(leaseFrame);

    synchronized (this) {
      final Lease lease =
          Lease.create(Duration.ofMillis(timeToLiveMillis), numberOfRequests, metadata);
      final Queue<LeasePermitHandler> queue = this.awaitingPermitHandlersQueue;

      int availableRequests = lease.numberOfRequests();

      this.currentLease = lease;
      if (queue.size() > 0) {
        do {
          final LeasePermitHandler handler = queue.poll();
          if (handler.handlePermit()) {
            availableRequests--;
          }
        } while (availableRequests > 0 && queue.size() > 0);
      }

      this.availableRequests = availableRequests;
    }
  }

  public synchronized void dispose(Throwable t) {
    this.isDisposed = true;
    this.t = t;

    final Queue<LeasePermitHandler> queue = this.awaitingPermitHandlersQueue;
    final int size = queue.size();

    for (int i = 0; i < size; i++) {
      final LeasePermitHandler leasePermitHandler = queue.poll();

      //noinspection ConstantConditions
      leasePermitHandler.handlePermitError(t);
    }
  }

  @Override
  public synchronized double availability() {
    final Lease lease = this.currentLease;
    return lease != null ? this.availableRequests / (double) lease.numberOfRequests() : 0.0d;
  }

  static boolean isExpired(Lease currentLease) {
    return System.currentTimeMillis() >= currentLease.expirationTime();
  }
}
