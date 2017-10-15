package io.rsocket.lease;

import io.rsocket.Availability;
import io.rsocket.Closeable;
import io.rsocket.RSocket;
import java.nio.ByteBuffer;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import reactor.core.publisher.Mono;

/** Provides means to grant lease to peer and check RSocket responder availability */
public class LeaseRSocketRef implements Availability {
  private static final CloseableAvailability closed = new ClosedAvailability();
  private final LeaseGranter leaseGranter;
  private volatile CloseableAvailability rsocket;
  private volatile boolean isClosed;

  LeaseRSocketRef(@Nonnull LeaseGranter leaseGranter, @Nonnull RSocket responderRSocket) {
    Objects.requireNonNull(leaseGranter, "leaseGranter");
    Objects.requireNonNull(responderRSocket, "responderRSocket");
    this.leaseGranter = leaseGranter;
    this.rsocket = wrap(responderRSocket);
    this.rsocket
        .onClose()
        .doOnTerminate(
            () -> {
              this.isClosed = true;
              this.rsocket = closed;
            })
        .subscribe();
  }

  public double availability() {
    return rsocket.availability();
  }

  public boolean isClosed() {
    return isClosed;
  }

  public void grantLease(int numberOfRequests, long timeToLive, @Nullable ByteBuffer metadata) {
    assertArgs(numberOfRequests, timeToLive);
    leaseGranter.grantLease(numberOfRequests, Math.toIntExact(timeToLive), metadata);
  }

  public void grantLease(int numberOfRequests, long timeToLive) {
    grantLease(numberOfRequests, timeToLive, null);
  }

  public void withdrawLease() {
    grantLease(0, 0);
  }

  Mono<Void> onClose() {
    return rsocket.onClose();
  }

  private void assertArgs(int numberOfRequests, long ttl) {
    if (numberOfRequests < 0) {
      throw new IllegalArgumentException("numberOfRequests should be non-negative");
    }

    if (ttl < 0) {
      throw new IllegalArgumentException("timeToLive should be non-negative");
    }
  }

  private static CloseableAvailability wrap(RSocket rSocket) {
    return new CloseableAvailability() {
      @Override
      public double availability() {
        return rSocket.availability();
      }

      @Override
      public Mono<Void> close() {
        return rSocket.close();
      }

      @Override
      public Mono<Void> onClose() {
        return rSocket.onClose();
      }
    };
  }

  private interface CloseableAvailability extends Closeable, Availability {}

  private static class ClosedAvailability implements CloseableAvailability {
    @Override
    public double availability() {
      return 0;
    }

    @Override
    public Mono<Void> close() {
      return Mono.empty();
    }

    @Override
    public Mono<Void> onClose() {
      return Mono.empty();
    }
  }
}
