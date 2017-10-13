package io.rsocket.lease;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.RSocketProxy;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;

/**
 * Protects rsocket from requests without lease, and maintains its availability based on current leases and availability
 * of underlying rsocket
 */
class LeaseRSocket extends RSocketProxy {
    private final LeaseManager leaseManager;
    private final String tag;

    LeaseRSocket(
            @Nonnull RSocket source, @Nonnull LeaseManager leaseManager, @Nonnull String tag) {
        super(source);
        this.leaseManager = leaseManager;
        this.tag = tag;
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
        return protectedRequest(super.fireAndForget(payload));
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
        return protectedRequest(super.requestResponse(payload));
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
        return protectedRequest(super.requestStream(payload));
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
        return protectedRequest(super.requestChannel(payloads));
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

    private <T> Mono<T> protectedRequest(Mono<T> request) {
        return Mono.<Lease>just(leaseManager.getLease())
                .flatMap(lease -> {
                    if (lease.isValid()) {
                        leaseManager.useLease();
                        return request;
                    } else {
                        return Mono.error(new NoLeaseException(lease));
                    }
                });
    }

    private <T> Flux<T> protectedRequest(Flux<T> request) {
        return Flux.<Lease>just(leaseManager.getLease())
                .flatMap(lease -> {
                    if (lease.isValid()) {
                        leaseManager.useLease();
                        return request;
                    } else {
                        return Flux.error(new NoLeaseException(lease));
                    }
                });
    }
}
