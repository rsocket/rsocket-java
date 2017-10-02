package io.rsocket.lease;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import reactor.core.publisher.*;

class LeaseManager {
  private static final Lease INVALID_LEASE = new LeaseImpl(0, 0);
  private static final MutableLease INVALID_MUTABLE_LEASE = new MutableLease(INVALID_LEASE);

  private volatile MutableLease currentLease = INVALID_MUTABLE_LEASE;
  private final ReplayProcessor<Lease> leaseSource = ReplayProcessor.create(1);
  private final Queue<Runnable> leasesQueue = new ConcurrentLinkedQueue<>();
  private final AtomicInteger leasesQueueWip = new AtomicInteger();
  private final String tag;

  LeaseManager(@Nonnull String tag, @Nonnull Mono<Void> connectionCloseSignal) {
    this.tag = tag;
    this.leaseSource.onNext(INVALID_LEASE);
    connectionCloseSignal
        .doOnTerminate(() -> dispatchLeaseChanged(INVALID_MUTABLE_LEASE))
        .subscribe();
  }

  LeaseManager(@Nonnull String tag) {
    this(tag, Mono.never());
  }

  public Flux<Lease> getLeases() {
    return leaseSource;
  }

  public Mono<Lease> getLease() {
    return getLeases().next();
  }

  public void leaseGranted(@Nonnull Lease lease) {
    Objects.requireNonNull(lease, "lease");
    leaseGranted(lease.getAllowedRequests(), lease.getTtl(), lease.getMetadata());
  }

  public void leaseGranted(int numberOfRequests, int ttl, @Nullable ByteBuffer metadata) {
    assertGrantedLease(numberOfRequests, ttl);
    MutableLease oldLease = currentLease;
    MutableLease newLease = new MutableLease(numberOfRequests, ttl, metadata);
    this.currentLease = newLease;
    if (oldLease.isValid() || newLease.isValid()) {
      dispatchLeaseChanged(newLease);
    }
  }

  public void useLease(int numOfRequests) {
    MutableLease cur = currentLease;
    boolean wasValid = cur.isValid();
    cur.use(numOfRequests);
    if (wasValid) {
      dispatchLeaseChanged(cur);
    }
  }

  public void useLease() {
    useLease(1);
  }

  private void dispatchLeaseChanged(MutableLease mutableLease) {
    leasesQueue.offer(() -> nextLease(mutableLease));
    if (leasesQueueWip.getAndIncrement() == 0) {
      do {
        leasesQueue.poll().run();
      } while (leasesQueueWip.decrementAndGet() != 0);
    }
  }

  private void nextLease(MutableLease mutableLease) {
    if (mutableLease == INVALID_MUTABLE_LEASE) {
      leaseSource.onNext(INVALID_LEASE);
      leaseSource.onComplete();
    } else {
      leaseSource.onNext(
          new LeaseImpl(
              mutableLease.getNumberOfRequests(),
              mutableLease.getTtl(),
              mutableLease.getMetadata()));
    }
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

  private static class MutableLease {
    private final int ttl;
    private final AtomicInteger numberOfRequests;
    private final ByteBuffer metadata;
    private final long expiry;

    public MutableLease(int numberOfRequests, int ttl, @Nullable ByteBuffer metadata) {
      assertNumberOfRequests(numberOfRequests);
      this.numberOfRequests = new AtomicInteger(numberOfRequests);
      this.ttl = ttl;
      this.metadata = metadata;
      this.expiry = now() + ttl;
    }

    public MutableLease(Lease lease) {
      this(lease.getAllowedRequests(), lease.getTtl(), lease.getMetadata());
    }

    public MutableLease use(int useRequestCount) {
      assertUseRequests(useRequestCount);
      numberOfRequests.accumulateAndGet(
          useRequestCount, (cur, update) -> Math.max(0, cur - update));
      return this;
    }

    public int getTtl() {
      return ttl;
    }

    public int getNumberOfRequests() {
      return numberOfRequests.get();
    }

    public ByteBuffer getMetadata() {
      return metadata;
    }

    public long getExpiry() {
      return expiry;
    }

    public boolean isValid() {
      return now() <= getExpiry() && getNumberOfRequests() > 0;
    }

    private long now() {
      return System.currentTimeMillis();
    }

    private static void assertUseRequests(int useRequestCount) {
      if (useRequestCount <= 0) {
        throw new IllegalArgumentException("Number of requests should be positive");
      }
    }

    private static void assertNumberOfRequests(int numberOfRequest) {
      if (numberOfRequest < 0) {
        throw new IllegalArgumentException("Number of requests should be positive");
      }
    }
  }
}
