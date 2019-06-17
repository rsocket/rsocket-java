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
import io.rsocket.exceptions.MissingLeaseException;
import io.rsocket.frame.LeaseFrameFlyweight;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;

public interface ResponderLeaseHandler extends Availability {

  boolean useLease();

  Exception leaseError();

  Disposable send(Consumer<ByteBuf> leaseFrameSender);

  final class Impl<T extends LeaseStats> implements ResponderLeaseHandler {
    private volatile LeaseImpl currentLease = LeaseImpl.empty();
    private final String tag;
    private final ByteBufAllocator allocator;
    private final Function<Optional<T>, Flux<Lease>> leaseSender;
    private final Consumer<Throwable> errorConsumer;
    private final Optional<T> leaseStatsOption;
    private final T leaseStats;

    public Impl(
        String tag,
        ByteBufAllocator allocator,
        Function<Optional<T>, Flux<Lease>> leaseSender,
        Consumer<Throwable> errorConsumer,
        Optional<T> leaseStatsOption) {
      this.tag = tag;
      this.allocator = allocator;
      this.leaseSender = leaseSender;
      this.errorConsumer = errorConsumer;
      this.leaseStatsOption = leaseStatsOption;
      this.leaseStats = leaseStatsOption.orElse(null);
    }

    @Override
    public boolean useLease() {
      boolean success = currentLease.use();
      onUseEvent(success, leaseStats);
      return success;
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
          .apply(leaseStatsOption)
          .doOnTerminate(this::onTerminateEvent)
          .subscribe(
              lease -> {
                currentLease = create(lease);
                leaseFrameSender.accept(createLeaseFrame(lease));
              },
              errorConsumer);
    }

    @Override
    public double availability() {
      return currentLease.availability();
    }

    private ByteBuf createLeaseFrame(Lease lease) {
      return LeaseFrameFlyweight.encode(
          allocator, lease.getTimeToLiveMillis(), lease.getAllowedRequests(), lease.getMetadata());
    }

    private void onTerminateEvent() {
      T ls = leaseStats;
      if (ls != null) {
        ls.onEvent(LeaseStats.EventType.TERMINATE);
      }
    }

    private void onUseEvent(boolean success, @Nullable T ls) {
      if (ls != null) {
        LeaseStats.EventType eventType =
            success ? LeaseStats.EventType.ACCEPT : LeaseStats.EventType.REJECT;
        ls.onEvent(eventType);
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
        public boolean useLease() {
          return true;
        }

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
