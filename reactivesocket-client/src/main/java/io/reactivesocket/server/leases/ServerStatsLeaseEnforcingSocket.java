package io.reactivesocket.server.leases;


import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.exceptions.RejectedException;
import io.reactivesocket.lease.DefaultLeaseEnforcingSocket;
import io.reactivesocket.reactivestreams.extensions.Px;
import org.reactivestreams.Publisher;

import java.util.function.LongSupplier;

/**
 * Extends {@code DefautLeaseEnforcingSocket} and records stats that can be used to
 * report, or adjust the leases.
 */
public class ServerStatsLeaseEnforcingSocket extends DefaultLeaseEnforcingSocket {
    private final ReactiveSocketServerStats stats;

    public ServerStatsLeaseEnforcingSocket(
        ReactiveSocket delegate,
        ReactiveSocketServerStats stats,
        LeaseDistributor leaseDistributor,
        LongSupplier currentTimeSupplier,
        boolean clientHonorsLeases) {
        super(delegate, leaseDistributor, currentTimeSupplier, clientHonorsLeases);
        this.stats = stats;
    }

    public ServerStatsLeaseEnforcingSocket(
        ReactiveSocket delegate,
        ReactiveSocketServerStats stats,
        LeaseDistributor leaseDistributor,
        LongSupplier currentTimeSupplier) {
        super(delegate, leaseDistributor, currentTimeSupplier);
        this.stats = stats;
    }

    public ServerStatsLeaseEnforcingSocket(
        ReactiveSocket delegate,
        ReactiveSocketServerStats stats,
        LeaseDistributor leaseDistributor) {
        super(delegate, leaseDistributor);
        this.stats = stats;
    }

    @Override
    public Publisher<Void> fireAndForget(Payload payload) {
        stats.incrementOutstanding();
        return Px
            .from(super.fireAndForget(payload))
            .doOnError(t -> {
                if (t instanceof RejectedException) {
                    stats.incrementRejected();
                }
            })
            .doOnComplete(stats::incrementSuccessful)
            .doFinally(stats::incrementProcessed);
    }

    @Override
    public Publisher<Payload> requestResponse(Payload payload) {
        stats.incrementOutstanding();
        return Px
            .from(super.requestResponse(payload))
            .doOnError(t -> {
                if (t instanceof RejectedException) {
                    stats.incrementRejected();
                }
            })
            .doOnComplete(stats::incrementSuccessful)
            .doFinally(stats::incrementProcessed);
    }

    @Override
    public Publisher<Payload> requestStream(Payload payload) {
        stats.incrementOutstanding();
        return Px
            .from(super.requestStream(payload))
            .doOnError(t -> {
                if (t instanceof RejectedException) {
                    stats.incrementRejected();
                }
            })
            .doOnComplete(stats::incrementSuccessful)
            .doFinally(stats::incrementProcessed);
    }

    @Override
    public Publisher<Payload> requestSubscription(Payload payload) {
        stats.incrementOutstanding();
        return Px
            .from(super.requestSubscription(payload))
            .doOnError(t -> {
                if (t instanceof RejectedException) {
                    stats.incrementRejected();
                }
            })
            .doOnComplete(stats::incrementSuccessful)
            .doFinally(stats::incrementProcessed);
    }

    @Override
    public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
        stats.incrementOutstanding();
        return Px
            .from(super.requestChannel(payloads))
            .doOnError(t -> {
                if (t instanceof RejectedException) {
                    stats.incrementRejected();
                }
            })
            .doOnComplete(stats::incrementSuccessful)
            .doFinally(stats::incrementProcessed);
    }

    @Override
    public Publisher<Void> metadataPush(Payload payload) {
        stats.incrementOutstanding();
        return Px
            .from(super.metadataPush(payload))
            .doOnError(t -> {
                if (t instanceof RejectedException) {
                    stats.incrementRejected();
                }
            })
            .doOnComplete(stats::incrementSuccessful)
            .doFinally(stats::incrementProcessed);
    }
}
