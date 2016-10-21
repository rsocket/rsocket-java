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

import io.reactivesocket.Frame;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.reactivestreams.extensions.Px;
import io.reactivesocket.reactivestreams.extensions.internal.Cancellable;
import io.reactivesocket.reactivestreams.extensions.internal.CancellableImpl;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.IntSupplier;

/**
 * A distributor of leases to multiple {@link ReactiveSocket} instances based on the capacity as provided by the
 * supplied {@code IntSupplier}, regularly on every tick as provided by the supplied {@code Publisher}
 */
public final class FairLeaseDistributor implements DefaultLeaseEnforcingSocket.LeaseDistributor {

    private final LinkedBlockingQueue<Consumer<Lease>> activeRecipients;
    private Subscription ticksSubscription;
    private final int leaseTTL;
    private volatile int remainingPermits;

    public FairLeaseDistributor(IntSupplier capacitySupplier, int leaseTTL, Publisher<Long> leaseDistributionTicks) {
        this.leaseTTL = leaseTTL;
        activeRecipients = new LinkedBlockingQueue<>();
        Px.from(leaseDistributionTicks)
            .doOnSubscribe(subscription -> ticksSubscription = subscription)
                              .doOnNext(aLong -> {
                                  remainingPermits = capacitySupplier.getAsInt();
                                  distribute(remainingPermits);
                              })
                              .ignore()
                              .subscribe();
    }

    /**
     * Shutdown this distributor. No more leases will be provided to the registered sockets.
     */
    @Override
    public void shutdown() {
        ticksSubscription.cancel();
    }

    /**
     * Registers a socket (lease consumer) to this distributor. If there are any permits available, they will be
     * provided to this socket, otherwise, a fair share will be provided on the next distribution.
     *
     * @param leaseConsumer Consumer of the leases (usually the registered socket).
     *
     * @return A handle to cancel this registration, when the socket is closed.
     */
    @Override
    public Cancellable registerSocket(Consumer<Lease> leaseConsumer) {
        activeRecipients.add(leaseConsumer);
        return new CancellableImpl() {
            @Override
            protected void onCancel() {
                activeRecipients.remove(leaseConsumer);
            }
        };
    }

    private void distribute(int permits) {
        if (activeRecipients.isEmpty()) {
            return;
        }
        remainingPermits -= permits;
        int recipients = activeRecipients.size();
        int budget = permits / recipients;

        // it would be more fair to randomized the distribution of extra
        int extra = permits - budget * recipients;
        Lease budgetLease = new LeaseImpl(budget, leaseTTL, Frame.NULL_BYTEBUFFER);
        for (Consumer<Lease> recipient: activeRecipients) {
            Lease leaseToSend = budgetLease;
            int n = budget;
            if (extra > 0) {
                n += 1;
                extra -= 1;
                leaseToSend = new LeaseImpl(n, leaseTTL, Frame.NULL_BYTEBUFFER);
            }
            recipient.accept(leaseToSend);
        }
    }
}
