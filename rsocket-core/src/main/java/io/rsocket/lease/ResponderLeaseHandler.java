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
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Availability;
import io.rsocket.frame.LeaseFrameCodec;
import java.util.function.Consumer;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.util.annotation.Nullable;

public interface ResponderLeaseHandler extends Availability {

  boolean useLease();

  Exception leaseError();

  Disposable send(Consumer<ByteBuf> leaseFrameSender, @Nullable RequestTracker requestTracker);

  @SuppressWarnings("rawtypes")
  final class Impl implements ResponderLeaseHandler {
    private volatile LeaseImpl currentLease = LeaseImpl.empty();
    private final String tag;
    private final ByteBufAllocator allocator;
    private final LeaseSender leaseSender;

    public Impl(String tag, ByteBufAllocator allocator, LeaseSender leaseSender) {
      this.tag = tag;
      this.allocator = allocator;
      this.leaseSender = leaseSender;
    }

    @Override
    public boolean useLease() {
      return currentLease.use();
    }

    @Override
    public Exception leaseError() {
      LeaseImpl l = currentLease;
      String t = tag;
      if (!l.isValid()) {
        return new MissingLeaseException(l, t);
      } else {
        return new MissingLeaseException(t);
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Disposable send(Consumer<ByteBuf> leaseFrameSender, RequestTracker requestTracker) {
      return leaseSender
          .send(requestTracker)
          .subscribe(
              new Consumer<Lease>() {
                @Override
                public void accept(Lease lease) {
                  currentLease = create(lease);
                  leaseFrameSender.accept(Impl.this.createLeaseFrame(lease));
                }
              });
    }

    @Override
    public double availability() {
      return currentLease.availability();
    }

    private ByteBuf createLeaseFrame(Lease lease) {
      return LeaseFrameCodec.encode(
          allocator, lease.getTimeToLiveMillis(), lease.getAllowedRequests(), lease.getMetadata());
    }

    private static LeaseImpl create(Lease lease) {
      if (lease instanceof LeaseImpl) {
        return (LeaseImpl) lease;
      } else {
        return LeaseImpl.create(
            lease.getTimeToLiveMillis(), lease.getAllowedRequests(), lease.getMetadata());
      }
    }
  }

  @Deprecated
  ResponderLeaseHandler None =
      new ResponderLeaseHandler() {
        @Override
        public boolean useLease() {
          return true;
        }

        @Override
        public Exception leaseError() {
          throw new AssertionError("Error not possible with NOOP leases handler");
        }

        @Override
        public Disposable send(Consumer<ByteBuf> leaseFrameSender, RequestTracker requestTracker) {
          return Disposables.disposed();
        }

        @Override
        public double availability() {
          return 1.0;
        }
      };
}
