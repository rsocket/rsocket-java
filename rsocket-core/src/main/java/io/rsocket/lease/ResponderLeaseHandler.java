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
import io.rsocket.frame.FrameType;
import io.rsocket.frame.LeaseFrameCodec;
import java.util.function.Consumer;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.SignalType;
import reactor.util.annotation.Nullable;

public interface ResponderLeaseHandler extends Availability {

  boolean useLease(int streamId, FrameType requestType, @Nullable ByteBuf metadata);

  void releaseLease(int streamId, SignalType releaseSignal);

  Exception leaseError();

  Disposable send(Consumer<ByteBuf> leaseFrameSender);

  final class Impl<T extends LeaseTracker> implements ResponderLeaseHandler {
    private volatile LeaseImpl currentLease = LeaseImpl.empty();
    private final String tag;
    private final ByteBufAllocator allocator;
    private final LeaseSender<T> leaseSender;
    private final T leaseTracker;

    public Impl(
        String tag,
        ByteBufAllocator allocator,
        LeaseSender<T> leaseSender,
        @Nullable T leaseTracker) {
      this.tag = tag;
      this.allocator = allocator;
      this.leaseSender = leaseSender;
      this.leaseTracker = leaseTracker;
    }

    @Override
    public boolean useLease(int streamId, FrameType requestType, @Nullable ByteBuf metadata) {
      boolean success = currentLease.use();

      T leaseStats = this.leaseTracker;
      if (leaseStats != null) {
        if (success) {
          leaseStats.onAccept(streamId, requestType, metadata);
        } else {
          leaseStats.onReject(streamId, requestType, metadata);
        }
      }

      return success;
    }

    @Override
    public void releaseLease(int streamId, SignalType releaseSignal) {
      T leaseStats = this.leaseTracker;
      if (leaseStats != null) {
        leaseStats.onRelease(streamId, releaseSignal);
      }
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
    public Disposable send(Consumer<ByteBuf> leaseFrameSender) {
      return leaseSender
          .send(leaseTracker)
          .doFinally(this::onTerminateEvent)
          .subscribe(
              lease -> {
                currentLease = create(lease);
                leaseFrameSender.accept(createLeaseFrame(lease));
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

    private void onTerminateEvent(SignalType signalType) {
      T ls = leaseTracker;
      if (ls != null) {
        ls.onClose();
      }
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

  ResponderLeaseHandler None =
      new ResponderLeaseHandler() {
        @Override
        public boolean useLease(int streamId, FrameType frameType, @Nullable ByteBuf metadata) {
          return true;
        }

        @Override
        public void releaseLease(int streamId, SignalType releaseSignal) {}

        @Override
        public Exception leaseError() {
          throw new AssertionError("Error not possible with NOOP leases handler");
        }

        @Override
        public Disposable send(Consumer<ByteBuf> leaseFrameSender) {
          return Disposables.disposed();
        }

        @Override
        public double availability() {
          return 1.0;
        }
      };
}
