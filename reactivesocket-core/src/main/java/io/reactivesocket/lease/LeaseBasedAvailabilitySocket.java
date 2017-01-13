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

import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.reactivestreams.extensions.Px;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Doesn't reject the request when leases are zero, just lowers the availability to zero.
 */
public class LeaseBasedAvailabilitySocket implements LeaseHonoringSocket {

    private static final Logger logger = LoggerFactory.getLogger(LeaseBasedAvailabilitySocket.class);

    private volatile Lease currentLease;
    private final ReactiveSocket delegate;
    private final AtomicInteger remainingQuota;
    private final AtomicInteger concurrency;
    private volatile int concurrencyLimit = 0;

    public LeaseBasedAvailabilitySocket(ReactiveSocket delegate) {
        this.delegate = delegate;
        this.remainingQuota = new AtomicInteger();
        this.concurrency = new AtomicInteger();
    }

    @Override
    public void accept(Lease lease) {
        logger.debug("accepting lease allowed requests => {}, ttl => {}", lease.getAllowedRequests(), lease.getTtl());
        currentLease = lease;
        remainingQuota.set(lease.getAllowedRequests());

        ByteBuffer metadata = lease.metadata();
        if (metadata != null) {
            metadata.rewind();
            concurrencyLimit = metadata.getInt(0);
        } else {
            concurrencyLimit = Runtime.getRuntime().availableProcessors() * 10;
        }
    }

    @Override
    public Publisher<Void> fireAndForget(Payload payload) {
        remainingQuota.decrementAndGet();
        concurrency.incrementAndGet();
        return Px.from(delegate.fireAndForget(payload)).doFinally(concurrency::decrementAndGet);
    }

    @Override
    public Publisher<Payload> requestResponse(Payload payload) {
        remainingQuota.decrementAndGet();
        concurrency.incrementAndGet();
        return Px.from(delegate.requestResponse(payload)).doFinally(concurrency::decrementAndGet);
    }

    @Override
    public Publisher<Payload> requestStream(Payload payload) {
        remainingQuota.decrementAndGet();
        concurrency.incrementAndGet();
        return Px.from(delegate.requestStream(payload)).doFinally(concurrency::decrementAndGet);
    }

    @Override
    public Publisher<Payload> requestSubscription(Payload payload) {
        remainingQuota.decrementAndGet();
        concurrency.incrementAndGet();
        return Px.from(delegate.requestSubscription(payload)).doFinally(concurrency::decrementAndGet);
    }

    @Override
    public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
        remainingQuota.decrementAndGet();
        concurrency.incrementAndGet();
        return Px.from(delegate.requestChannel(payloads)).doFinally(concurrency::decrementAndGet);
    }

    @Override
    public Publisher<Void> metadataPush(Payload payload) {
        remainingQuota.decrementAndGet();
        concurrency.incrementAndGet();
        return Px.from(delegate.metadataPush(payload)).doFinally(concurrency::decrementAndGet);
    }

    @Override
    public double availability() {
        if (currentLease == null || currentLease.getAllowedRequests() <= 0 || currentLease.isExpired()) {
            return 0.0;
        } else {
            return
                delegate.availability()
                    * ((double) remainingQuota.get() / (double) currentLease.getAllowedRequests())
                    * computeConcurrencyAvailability();
        }
    }

    private double computeConcurrencyAvailability() {
        double availability;
        int currentLimit = concurrencyLimit;
        if (currentLimit <= 0) {
            availability = 0;
        } else {
            availability = Math.max((currentLimit - concurrency.get()), 0) / (double) currentLimit;
        }

        return availability;

    }

    @Override
    public Publisher<Void> close() {
        return delegate.close();
    }

    @Override
    public Publisher<Void> onClose() {
        return delegate.onClose();
    }

}
