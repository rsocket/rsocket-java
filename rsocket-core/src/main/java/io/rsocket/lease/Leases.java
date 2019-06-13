package io.rsocket.lease;

import java.util.function.Consumer;
import java.util.function.Function;
import reactor.core.publisher.Flux;

public class Leases {
  private static final Function<LeaseStats, Flux<Lease>> noopLeaseSender =
      leaseStats -> Flux.never();
  private static final Consumer<Flux<Lease>> noopLeaseReceiver = leases -> {};

  private Function<LeaseStats, Flux<Lease>> leaseSender = noopLeaseSender;
  private Consumer<Flux<Lease>> leaseReceiver = noopLeaseReceiver;

  public static Leases create() {
    return new Leases();
  }

  public Leases sender(Function<LeaseStats, Flux<Lease>> leaseSender) {
    this.leaseSender = leaseSender;
    return this;
  }

  public Leases receiver(Consumer<Flux<Lease>> leaseReceiver) {
    this.leaseReceiver = leaseReceiver;
    return this;
  }

  public Function<LeaseStats, Flux<Lease>> sender() {
    return leaseSender;
  }

  public Consumer<Flux<Lease>> receiver() {
    return leaseReceiver;
  }
}
