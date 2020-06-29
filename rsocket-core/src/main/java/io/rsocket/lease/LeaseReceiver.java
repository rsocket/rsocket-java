package io.rsocket.lease;

import reactor.core.publisher.Flux;

public interface LeaseReceiver {

  void receive(Flux<Lease> leases);
}
