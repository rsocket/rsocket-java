package io.rsocket.lease;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.exceptions.NoLeaseException;
import io.rsocket.util.RSocketProxy;
import javax.annotation.Nonnull;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Protects RSocket from requests without lease, maintains RSocket availability based on current
 * lease and availability of underlying rsocket
 */
class LeaseRSocket extends RSocketProxy {
  private final LeaseManager leaseManager;
  private final String tag;

  LeaseRSocket(@Nonnull RSocket source, @Nonnull LeaseManager leaseManager, @Nonnull String tag) {
    super(source);
    this.leaseManager = leaseManager;
    this.tag = tag;
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return leaseRequest(super.fireAndForget(payload));
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return leaseRequest(super.requestResponse(payload));
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return leaseRequest(super.requestStream(payload));
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return leaseRequest(super.requestChannel(payloads));
  }

  @Override
  public String toString() {
    return "LeaseRSocket{" + "tag=" + tag + '}';
  }

  @Override
  public double availability() {
    LeaseImpl lease = leaseManager.getLease();
    return lease.isValid()
        ? lease.getAllowedRequests() / (double) lease.getStartingAllowedRequests()
        : 0.0;
  }

  private <T> Mono<T> leaseRequest(Mono<T> request) {
    return Mono.<Lease>just(leaseManager.getLease())
        .flatMap(
            lease -> {
              if (lease.isValid()) {
                leaseManager.useLease();
                return request;
              } else {
                return Mono.error(new NoLeaseException(lease));
              }
            });
  }

  private <T> Flux<T> leaseRequest(Flux<T> request) {
    return Flux.<Lease>just(leaseManager.getLease())
        .flatMap(
            lease -> {
              if (lease.isValid()) {
                leaseManager.useLease();
                return request;
              } else {
                return Flux.error(new NoLeaseException(lease));
              }
            });
  }
}
