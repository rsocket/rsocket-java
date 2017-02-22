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
import io.reactivesocket.exceptions.RejectedException;
import io.reactivesocket.reactivestreams.extensions.Px;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

public class DefaultLeaseHonoringSocket implements LeaseHonoringSocket {

    private static final Logger logger = LoggerFactory.getLogger(DefaultLeaseHonoringSocket.class);

    private volatile Lease currentLease;
    private final ReactiveSocket delegate;
    private final LongSupplier currentTimeSupplier;
    private final AtomicInteger remainingQuota;

    @SuppressWarnings("ThrowableInstanceNeverThrown")
    private static final RejectedException rejectedException = new RejectedException("Lease exhausted.");
    @SuppressWarnings("rawtypes")
    private static final Px rejectedPx = Px.error(rejectedException);

    public DefaultLeaseHonoringSocket(ReactiveSocket delegate, LongSupplier currentTimeSupplier) {
        this.delegate = delegate;
        this.currentTimeSupplier = currentTimeSupplier;
        remainingQuota = new AtomicInteger();
    }

    public DefaultLeaseHonoringSocket(ReactiveSocket delegate) {
        this(delegate, System::currentTimeMillis);
    }

    @Override
    public void accept(Lease lease) {
        currentLease = lease;
        remainingQuota.set(lease.getAllowedRequests());
    }

    @Override
    public Publisher<Void> fireAndForget(Payload payload) {
        return Px.defer(() -> {
            if (!checkLease()) {
                return rejectError();
            }
            return delegate.fireAndForget(payload);
        });
    }

    @Override
    public Publisher<Payload> requestResponse(Payload payload) {
        return Px.defer(() -> {
            if (!checkLease()) {
                return rejectError();
            }
            return delegate.requestResponse(payload);
        });
    }

    @Override
    public Publisher<Payload> requestStream(Payload payload) {
        return Px.defer(() -> {
            if (!checkLease()) {
                return rejectError();
            }
            return delegate.requestStream(payload);
        });
    }

    @Override
    public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
        return Px.defer(() -> {
            if (!checkLease()) {
                return rejectError();
            }
            return delegate.requestChannel(payloads);
        });
    }

    @Override
    public Publisher<Void> metadataPush(Payload payload) {
        return Px.defer(() -> {
            if (!checkLease()) {
                return rejectError();
            }
            return delegate.metadataPush(payload);
        });
    }

    @Override
    public double availability() {
        return remainingQuota.get() <= 0 || currentLease.isExpired() ? 0.0 : delegate.availability();
    }

    @Override
    public Publisher<Void> close() {
        return delegate.close();
    }

    @Override
    public Publisher<Void> onClose() {
        return delegate.onClose();
    }

    @SuppressWarnings("unchecked")
    protected  <T> Publisher<T> rejectError() {
        return rejectedPx;
    }

    private boolean checkLease() {
        boolean allow = remainingQuota.getAndDecrement() > 0 && !currentLease.isExpired(currentTimeSupplier.getAsLong());
        if (!allow) {
            if (logger.isDebugEnabled()) {
                logger.debug("Lease expired. Lease: " + currentLease + ", remaining quota: "
                             + Math.max(0, remainingQuota.get()) + ", current time (ms) "
                             + currentTimeSupplier.getAsLong());
            }
        }
        return allow;
    }
}
