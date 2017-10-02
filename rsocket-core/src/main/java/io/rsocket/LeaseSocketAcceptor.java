package io.rsocket;

import reactor.core.publisher.Mono;

public interface LeaseSocketAcceptor {

  Mono<RSocket> accept(ConnectionSetupPayload setup, LeaseRSocket sendingSocket);
}
