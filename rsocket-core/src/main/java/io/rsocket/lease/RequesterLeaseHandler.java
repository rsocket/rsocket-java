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
import io.rsocket.Availability;
import io.rsocket.frame.LeaseFrameCodec;
import reactor.core.Disposable;
import reactor.core.publisher.ReplayProcessor;

public interface RequesterLeaseHandler extends Availability, Disposable {

  boolean useLease();

  Exception leaseError();

  void receive(ByteBuf leaseFrame);

  void dispose();

  final class Impl implements RequesterLeaseHandler {
    private final String tag;
    private final ReplayProcessor<Lease> receivedLease;
    private volatile LeaseImpl currentLease = LeaseImpl.empty();

    public Impl(String tag, LeaseReceiver leaseReceiver) {
      this.tag = tag;
      receivedLease = ReplayProcessor.create(1);
      leaseReceiver.receive(receivedLease);
    }

    @Override
    public boolean useLease() {
      return currentLease.use();
    }

    @Override
    public Exception leaseError() {
      LeaseImpl l = this.currentLease;
      String t = this.tag;
      if (!l.isValid()) {
        return new MissingLeaseException(l, t);
      } else {
        return new MissingLeaseException(t);
      }
    }

    @Override
    public void receive(ByteBuf leaseFrame) {
      int numberOfRequests = LeaseFrameCodec.numRequests(leaseFrame);
      int timeToLiveMillis = LeaseFrameCodec.ttl(leaseFrame);
      ByteBuf metadata = LeaseFrameCodec.metadata(leaseFrame);
      LeaseImpl lease = LeaseImpl.create(timeToLiveMillis, numberOfRequests, metadata);
      currentLease = lease;
      receivedLease.onNext(lease);
    }

    @Override
    public void dispose() {
      receivedLease.onComplete();
    }

    @Override
    public boolean isDisposed() {
      return receivedLease.isTerminated();
    }

    @Override
    public double availability() {
      return currentLease.availability();
    }
  }

  RequesterLeaseHandler None =
      new RequesterLeaseHandler() {
        @Override
        public boolean useLease() {
          return true;
        }

        @Override
        public Exception leaseError() {
          throw new AssertionError("Error not possible with NOOP leases handler");
        }

        @Override
        public void receive(ByteBuf leaseFrame) {}

        @Override
        public void dispose() {}

        @Override
        public double availability() {
          return 1.0;
        }
      };
}
