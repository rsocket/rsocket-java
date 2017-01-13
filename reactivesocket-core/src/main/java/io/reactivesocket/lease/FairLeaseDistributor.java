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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.IntSupplier;

/**
 * A distributor of leases to multiple {@link ReactiveSocket} instances based on the capacity as provided by the
 * supplied {@code IntSupplier}, regularly on every tick as provided by the supplied {@code Publisher}
 */
public final class FairLeaseDistributor implements DefaultLeaseEnforcingSocket.LeaseDistributor {

    private static final Logger logger = LoggerFactory.getLogger(FairLeaseDistributor.class);
    private final List<Consumer<Lease>> activeRecipients;
    private Subscription ticksSubscription;
    private boolean ticksStarted;
    private final IntSupplier capacitySupplier;
    private final IntSupplier leaseTTLMillis;
    private final Publisher<Long> leaseDistributionTicks;
    private final boolean redistributeOnConnect;

    public FairLeaseDistributor(IntSupplier capacitySupplier, IntSupplier leaseTTLMillis,
                                Publisher<Long> leaseDistributionTicks, boolean redistributeOnConnect) {
        this.leaseTTLMillis = leaseTTLMillis;
        this.capacitySupplier = capacitySupplier;
        this.leaseDistributionTicks = leaseDistributionTicks;
        this.redistributeOnConnect = redistributeOnConnect;
        activeRecipients = new ArrayList<>();
    }

    public FairLeaseDistributor(IntSupplier capacitySupplier, IntSupplier leaseTTLMillis,
                                Publisher<Long> leaseDistributionTicks) {
        this(capacitySupplier, leaseTTLMillis, leaseDistributionTicks, true);
    }

    /**
     * Shutdown this distributor. No more leases will be provided to the registered sockets.
     */
    @Override
    public synchronized void shutdown() {
        if (activeRecipients.isEmpty()) {
            logger.debug("activeRecipients is empty, stopping ticker");
            ticksStarted = false;
            ticksSubscription.cancel();
        }
    }

    /**
     * Registers a socket (lease consumer) to this distributor. If there are any permits available, they will be
     * provided to this socket, otherwise, a fair share will be provided on the next distribution.
     *
     * @param leaseConsumer Consumer of the leases (usually the registered socket).
     * @return A handle to cancel this registration, when the socket is closed.
     */
    @Override
    public Cancellable registerSocket(Consumer<Lease> leaseConsumer) {
        logger.debug("registering socket => {}", leaseConsumer.toString());

        boolean _started;

        synchronized (this) {
            activeRecipients.add(leaseConsumer);

            _started = ticksStarted;
            if (!ticksStarted) {
                logger.debug("starting ticker");
                startTicks();
                ticksStarted = true;
            }
        }

        if (_started && redistributeOnConnect) {
            /*
             * This is a way to make sure that the clients that arrive in the middle of a distribution period, do not
             * have to wait for the next tick to arrive.
             */
            distribute(capacitySupplier.getAsInt());
        }

        return new CancellableImpl() {
            @Override
            protected void onCancel() {
                synchronized (FairLeaseDistributor.this) {
                    logger.debug("removing socket => {}", leaseConsumer.toString());
                    activeRecipients.remove(leaseConsumer);
                }
            }
        };
    }

    private synchronized void distribute(int permits) {
        if (activeRecipients.isEmpty()) {
            return;
        }

        final int recipients = activeRecipients.size();
        final int budget = permits / recipients;
        final int ttl = (int) (leaseTTLMillis.getAsInt() * 1.1);
        int extra = permits - budget * recipients;

        logger.debug("distribute recipients => {}, budget => {}, ttl => {}, extra permits => {}", recipients, budget, ttl, extra);

        // Pick a random starting point to fairly distributed leases
        //int start = ThreadLocalRandom.current().nextInt(0, recipients);

        Lease budgetLease = new LeaseImpl(budget, ttl, Frame.NULL_BYTEBUFFER);
        for (int i = 0; i < recipients; i++) {
            Consumer<Lease> recipient = activeRecipients.get(i);
            Lease leaseToSend = budgetLease;
            int n = budget;
            if (extra > 0) {
                n += 1;
                extra -= 1;
                leaseToSend = new LeaseImpl(n, ttl, Frame.NULL_BYTEBUFFER);
            }
            recipient.accept(leaseToSend);
        }
    }

    private void startTicks() {
        Px.from(leaseDistributionTicks)
            .doOnSubscribe(subscription -> ticksSubscription = subscription)
            .doOnNext(tick -> distribute(capacitySupplier.getAsInt()))
            .ignore()
            .subscribe();
    }
}
