/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.reactivesocket.lease;

import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.exceptions.RejectedException;
import io.reactivesocket.reactivestreams.extensions.internal.Cancellable;
import io.reactivesocket.reactivestreams.extensions.internal.subscribers.Subscribers;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;
import java.util.function.LongSupplier;

public class DefaultLeaseEnforcingSocket extends DefaultLeaseHonoringSocket implements LeaseEnforcingSocket {

    private final LeaseDistributor leaseDistributor;
    private volatile Consumer<Lease> leaseSender;
    private Cancellable distributorCancellation;
    private final Mono<?> rejectError;

    public DefaultLeaseEnforcingSocket(ReactiveSocket delegate, LeaseDistributor leaseDistributor,
                                       LongSupplier currentTimeSupplier, boolean clientHonorsLeases) {
        super(delegate, currentTimeSupplier);
        this.leaseDistributor = leaseDistributor;
        if (!clientHonorsLeases) {
            rejectError = Mono.error(new RejectedException("Server overloaded."));
        } else {
            rejectError = null;
        }
    }

    public DefaultLeaseEnforcingSocket(ReactiveSocket delegate, LeaseDistributor leaseDistributor,
                                       LongSupplier currentTimeSupplier) {
        this(delegate, leaseDistributor, currentTimeSupplier, true);
    }

    public DefaultLeaseEnforcingSocket(ReactiveSocket delegate, LeaseDistributor leaseDistributor) {
        this(delegate, leaseDistributor, System::currentTimeMillis);
    }

    @Override
    public void acceptLeaseSender(Consumer<Lease> leaseSender) {
        this.leaseSender = leaseSender;
        distributorCancellation = leaseDistributor.registerSocket(lease -> accept(lease));
        onClose().subscribe(Subscribers.doOnTerminate(() -> distributorCancellation.cancel()));
    }

    @Override
    public void accept(Lease lease) {
        leaseSender.accept(lease);
        super.accept(lease);
    }

    public LeaseDistributor getLeaseDistributor() {
        return leaseDistributor;
    }

    @Override
    public Mono<Void> close() {
        return super.close()
                 .doOnSubscribe(subscription -> {
                     leaseDistributor.shutdown();
                 });
    }

    @SuppressWarnings("unchecked")
    protected <T> Mono<T> rejectError() {
        return null == rejectError ? super.rejectError() : (Mono<T>) rejectError;
    }

    /**
     * A distributor of leases for an instance of {@link LeaseEnforcingSocket}.
     */
    public interface LeaseDistributor {

        /**
         * Shutdown this distributor.
         */
        void shutdown();

        /**
         * Registers a new socket (a consumer of lease) to this distributor. This registration can be canclled by
         * cancelling the returned {@link Cancellable}.
         *
         * @param leaseConsumer Consumer of lease.
         *
         * @return Cancellation handle. Call {@link Cancellable#cancel()} to unregister this socket.
         */
        Cancellable registerSocket(Consumer<Lease> leaseConsumer);
    }
}
