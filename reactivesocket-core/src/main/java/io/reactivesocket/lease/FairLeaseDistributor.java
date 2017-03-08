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
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

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
    private volatile boolean startTicks;
    private final IntSupplier capacitySupplier;
    private final int leaseTTLMillis;
    private final Flux<Long> leaseDistributionTicks;
    private final boolean redistributeOnConnect;

    public FairLeaseDistributor(IntSupplier capacitySupplier, int leaseTTLMillis,
                                Flux<Long> leaseDistributionTicks, boolean redistributeOnConnect) {
        this.capacitySupplier = capacitySupplier;
        /*
         * If lease TTL is exactly the same as the period of replenishment, then there would be a time period when new
         * lease has not arrived and old lease is expired. This isn't a good reflection of server's intent as the server
         * can handle the new requests but the lease has not yet reached the client. So, having TTL slightly more
         * than distribution period (accomodating for network lag) is more representative of server's intent. OTOH, if
         * server isn't ready, it can always reject a request.
         */
        this.leaseTTLMillis = (int) (leaseTTLMillis * 1.1);
        this.leaseDistributionTicks = leaseDistributionTicks;
        this.redistributeOnConnect = redistributeOnConnect;
        activeRecipients = new LinkedBlockingQueue<>();
    }

    public FairLeaseDistributor(IntSupplier capacitySupplier, int leaseTTLMillis,
                                Flux<Long> leaseDistributionTicks) {
        this(capacitySupplier, leaseTTLMillis, leaseDistributionTicks, true);
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
    public Disposable registerSocket(Consumer<Lease> leaseConsumer) {
        activeRecipients.add(leaseConsumer);
        boolean _started;
        synchronized (this) {
            _started = startTicks;
            if (!startTicks) {
                startTicks();
                startTicks = true;
            }
        }

        if (_started && redistributeOnConnect) {
            /*
             * This is a way to make sure that the clients that arrive in the middle of a distribution period, do not
             * have to wait for the next tick to arrive.
             */
            distribute(capacitySupplier.getAsInt());
        }

        return () -> activeRecipients.remove(leaseConsumer);
    }

    private void distribute(int permits) {
        if (activeRecipients.isEmpty()) {
            return;
        }
        int recipients = activeRecipients.size();
        int budget = permits / recipients;

        // it would be more fair to randomized the distribution of extra
        int extra = permits - budget * recipients;
        Lease budgetLease = new LeaseImpl(budget, leaseTTLMillis, Frame.NULL_BYTEBUFFER);
        for (Consumer<Lease> recipient: activeRecipients) {
            Lease leaseToSend = budgetLease;
            int n = budget;
            if (extra > 0) {
                n += 1;
                extra -= 1;
                leaseToSend = new LeaseImpl(n, leaseTTLMillis, Frame.NULL_BYTEBUFFER);
            }
            recipient.accept(leaseToSend);
        }
    }

    private void startTicks() {
        leaseDistributionTicks
          .doOnSubscribe(subscription -> ticksSubscription = subscription)
          .doOnNext(aLong -> {
              distribute(capacitySupplier.getAsInt());
          })
          .subscribe();
    }
}
