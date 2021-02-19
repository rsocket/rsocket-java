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
  final int allowedDeferredCallsNumber;
  final Queue<LeaseHandler> deferredCallsQueue;

  Lease currentLease = null;
  int availableRequests;

  boolean isDisposed;
  Throwable t;

  RequesterLeaseTracker(String tag, int allowedDeferredCallsNumber) {
    this.tag = tag;
    this.allowedDeferredCallsNumber = allowedDeferredCallsNumber;
    this.deferredCallsQueue = new ArrayDeque<>();
  }

  synchronized void issue(LeaseHandler frameHandler) {
    if (this.isDisposed) {
      frameHandler.handleError(this.t);
      return;
    }

    final int availableRequests = this.availableRequests;
    final Lease l = this.currentLease;
    final boolean leaseReceived = l != null;
    final boolean isExpired = leaseReceived && isExpired(l);

    if (leaseReceived && availableRequests > 0 && !isExpired) {
      this.availableRequests = availableRequests - 1;
      frameHandler.handleLease();
    } else {
      final Queue<LeaseHandler> queue = this.deferredCallsQueue;
      if (this.allowedDeferredCallsNumber > queue.size()) {
        queue.offer(frameHandler);
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
        frameHandler.handleError(t);
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
      final Queue<LeaseHandler> queue = this.deferredCallsQueue;

      int availableRequests = lease.allowedRequests();

      this.currentLease = lease;
      if (queue.size() > 0) {
        do {
          final LeaseHandler handler = queue.poll();
          handler.handleLease();
        } while (--availableRequests > 0 && queue.size() > 0);
      }

      this.availableRequests = availableRequests;
    }
  }

  public synchronized void dispose(Throwable t) {
    this.isDisposed = true;
    this.t = t;

    final Queue<LeaseHandler> queue = this.deferredCallsQueue;
    final int size = queue.size();

    for (int i = 0; i < size; i++) {
      final LeaseHandler leaseHandler = queue.poll();

      leaseHandler.handleError(t);
    }
  }

  @Override
  public synchronized double availability() {
    final Lease lease = this.currentLease;
    return lease != null ? this.availableRequests / (double) lease.allowedRequests() : 0.0d;
  }

  static boolean isExpired(Lease currentLease) {
    return System.currentTimeMillis() >= currentLease.expireAt();
  }
}
