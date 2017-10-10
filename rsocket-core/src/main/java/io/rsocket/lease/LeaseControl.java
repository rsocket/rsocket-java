package io.rsocket.lease;

import java.nio.ByteBuffer;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import reactor.core.publisher.Flux;

/** Provides means to grant and listen for lease changes from peer */
public class LeaseControl {
  private final LeaseManager requesterLeaseManager;
  private final LeaseGranter leaseGranter;

  LeaseControl(@Nonnull LeaseManager requesterLeaseManager, @Nonnull LeaseGranter leaseGranter) {
    Objects.requireNonNull(requesterLeaseManager, "requesterLeaseManager");
    Objects.requireNonNull(leaseGranter, "leaseGranter");
    this.requesterLeaseManager = requesterLeaseManager;
    this.leaseGranter = leaseGranter;
  }

  public Flux<Lease> getLeases() {
    return requesterLeaseManager.getLeases();
  }

  public void grantLease(int numberOfRequests, long timeToLive, @Nullable ByteBuffer metadata) {
    assertArgs(numberOfRequests, timeToLive);
    leaseGranter.grantLease(numberOfRequests, Math.toIntExact(timeToLive), metadata);
  }

  public void grantLease(int numberOfRequests, long timeToLive) {
    grantLease(numberOfRequests, timeToLive, null);
  }

  private void assertArgs(int numberOfRequests, long ttl) {
    if (numberOfRequests < 0) {
      throw new IllegalArgumentException("numberOfRequests should be non-negative");
    }

    if (ttl < 0) {
      throw new IllegalArgumentException("timeToLive should be non-negative");
    }
  }
}
