package io.rsocket.lease;

import reactor.core.publisher.Flux;

public interface LeaseSender {

  Flux<Lease> send();
}
