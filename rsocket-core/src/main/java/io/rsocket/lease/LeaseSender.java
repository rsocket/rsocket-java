package io.rsocket.lease;

import reactor.core.publisher.Flux;
import reactor.util.annotation.Nullable;

public interface LeaseSender<T extends RequestTracker> {

  Flux<Lease> send(@Nullable T leaseTracker);
}
