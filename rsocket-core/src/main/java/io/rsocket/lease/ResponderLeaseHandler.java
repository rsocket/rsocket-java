package io.rsocket.lease;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Availability;
import io.rsocket.exceptions.MissingLeaseException;
import io.rsocket.frame.LeaseFrameFlyweight;
import java.util.function.Consumer;
import java.util.function.Function;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;

public interface ResponderLeaseHandler extends Availability {

  boolean useLease();

  Exception leaseError();

  Disposable send(Consumer<ByteBuf> leaseFrameSender);

  final class Impl implements ResponderLeaseHandler {
    private volatile LeaseImpl currentLease = LeaseImpl.empty();
    private final LeaseStats leaseStats = new LeaseStats(currentLease);
    private final String tag;
    private final ByteBufAllocator allocator;
    private final Function<LeaseStats, Flux<Lease>> leaseSender;
    private final Consumer<Throwable> errorConsumer;
    private final int windowsCount;

    public Impl(
        String tag,
        ByteBufAllocator allocator,
        Function<LeaseStats, Flux<Lease>> leaseSender,
        Consumer<Throwable> errorConsumer,
        int statsWindowsCount) {
      this.tag = tag;
      this.allocator = allocator;
      this.leaseSender = leaseSender;
      this.errorConsumer = errorConsumer;
      this.windowsCount = statsWindowsCount;
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
    public Disposable send(Consumer<ByteBuf> leaseFrameSender) {
      return leaseSender
          .apply(leaseStats)
          .subscribe(
              lease -> {
                boolean isEmpty = currentLease.isEmpty();
                currentLease = LeaseImpl.create(lease);
                if (!isEmpty) {
                  leaseStats.stop();
                }
                leaseStats.start(
                    lease, Math.max(100, lease.getTimeToLiveMillis() / windowsCount), windowsCount);

                ByteBuf leaseFrame = createLeaseFrame(lease);
                leaseFrameSender.accept(leaseFrame);
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
  }

  ResponderLeaseHandler Noop =
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
