package io.rsocket.lease;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.RSocketProxy;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
/** Protects rsocket from requests without lease */
class LeaseEnforcer extends RSocketProxy {
  private final LeaseManager leaseManager;
  private final String tag;

  LeaseEnforcer(@Nonnull RSocket source, @Nonnull LeaseManager leaseManager, @Nonnull String tag) {
    super(source);
    this.leaseManager = leaseManager;
    this.tag = tag;
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return requestMono(() -> super.fireAndForget(payload));
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return requestMono(() -> super.requestResponse(payload));
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return requestFlux(() -> super.requestStream(payload));
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return requestFlux(() -> super.requestChannel(payloads));
  }

  @Override
  public String toString() {
    return "LeaseEnforcer{" + "tag=" + tag + '}';
  }

  private <T> Mono<T> requestMono(Supplier<Mono<T>> request) {
    return leaseManager
        .getLease()
        .flatMap(
            lease -> {
              if (lease.isValid()) {
                leaseManager.useLease();
                return request.get();
              } else {
                return Mono.error(new NoLeaseException(lease));
              }
            });
  }

  private <T> Flux<T> requestFlux(Supplier<Flux<T>> request) {
    return leaseManager
        .getLease()
        .flatMapMany(
            lease -> {
              if (lease.isValid()) {
                leaseManager.useLease();
                return request.get();
              } else {
                return Flux.error(new NoLeaseException(lease));
              }
            });
  }
}
