package io.rsocket.lease;

import java.nio.ByteBuffer;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import reactor.core.publisher.*;
/** Updates Lease on use and grant */
class LeaseManager {
  private static final MutableLeaseImpl INVALID_MUTABLE_LEASE = new MutableLeaseImpl(0, 0, null);
  private volatile MutableLeaseImpl currentLease = INVALID_MUTABLE_LEASE;
  private final String tag;

  LeaseManager(@Nonnull String tag, @Nonnull Mono<Void> connectionCloseSignal) {
    this.tag = tag;
    connectionCloseSignal.doOnTerminate(() -> currentLease = INVALID_MUTABLE_LEASE).subscribe();
  }

  LeaseManager(@Nonnull String tag) {
    this(tag, Mono.never());
  }

  LeaseImpl getLease() {
    return currentLease;
  }

  public void leaseGranted(@Nonnull Lease lease) {
    Objects.requireNonNull(lease, "lease");
    leaseGranted(lease.getAllowedRequests(), lease.getTtl(), lease.getMetadata());
  }

  public void leaseGranted(int numberOfRequests, int ttl, @Nullable ByteBuffer metadata) {
    assertGrantedLease(numberOfRequests, ttl);
    this.currentLease = new MutableLeaseImpl(numberOfRequests, ttl, metadata);
  }

  public void useLease() {
    currentLease.use(1);
  }

  private static void assertGrantedLease(int numberOfRequests, int ttl) {
    if (numberOfRequests < 0) {
      throw new IllegalArgumentException("numberOfRequests should be non-negative");
    }
    if (ttl < 0) {
      throw new IllegalArgumentException("ttl should be non-negative");
    }
  }

  @Override
  public String toString() {
    return "LeaseManager{" + "tag='" + tag + '\'' + '}';
  }

  private static class MutableLeaseImpl extends LeaseImpl {

    MutableLeaseImpl(int numberOfRequests, int ttl, @Nullable ByteBuffer metadata) {
      super(numberOfRequests, ttl, metadata);
    }

    void use(int useRequestCount) {
      assertUseRequests(useRequestCount);
      numberOfRequests.accumulateAndGet(
          useRequestCount, (cur, update) -> Math.max(0, cur - update));
    }

    static void assertUseRequests(int useRequestCount) {
      if (useRequestCount <= 0) {
        throw new IllegalArgumentException("Number of requests should be positive");
      }
    }
  }
}
