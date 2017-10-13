package io.rsocket;

import io.rsocket.lease.LeaseRSocketRef;
import reactor.core.publisher.Mono;
import reactor.util.annotation.NonNull;

public class LeaseClosable<T extends Closeable> implements Closeable {
  private final T closeable;
  private final Mono<LeaseRSocketRef> leaseControl;

  public LeaseClosable(@NonNull T closeable, @NonNull Mono<LeaseRSocketRef> leaseControl) {
    this.closeable = closeable;
    this.leaseControl = leaseControl;
  }

  public T getCloseable() {
    return closeable;
  }

  public Mono<LeaseRSocketRef> getLeaseControl() {
    return leaseControl;
  }

  @Override
  public Mono<Void> close() {
    return closeable.close();
  }

  @Override
  public Mono<Void> onClose() {
    return closeable.onClose();
  }
}
