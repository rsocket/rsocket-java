package io.rsocket.examples.transport.tcp.lease.advanced.common;

import io.rsocket.Payload;
import io.rsocket.lease.LeaseReceiver;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Requester-side Lease listener.<br>
 * In the nutshell this class implements mechanism to listen (and do appropriate actions as needed)
 * to incoming leases issued by the Responder
 */
public interface DeferringLeaseReceiver extends LeaseReceiver, Disposable {
  /**
   * Subscribes to the given source if lease has been acquired successfully.
   *
   * <p>If there are no available leases this method allows to listen to new incoming leases and
   * deffer a call (without doing some redundant action, e.g . retry) until new valid lease has come
   * in.
   */
  <T> Mono<T> acquireLease(Mono<T> source);

  /**
   * Subscribes to the given source if lease has been acquired successfully.
   *
   * <p>If there are no available leases this method allows to listen to new incoming leases and
   * deffer a call (without doing some redundant action, e.g . retry) until new valid lease has come
   * in.
   */
  Flux<Payload> acquireLease(Flux<Payload> source);
}
