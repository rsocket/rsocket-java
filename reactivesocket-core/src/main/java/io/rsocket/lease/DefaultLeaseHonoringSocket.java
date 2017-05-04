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

package io.rsocket.lease;

import io.rsocket.Payload;
import io.rsocket.ReactiveSocket;
import io.rsocket.exceptions.RejectedException;
import io.rsocket.util.ReactiveSocketProxy;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

public class DefaultLeaseHonoringSocket extends ReactiveSocketProxy implements LeaseHonoringSocket {

    private static final Logger logger = LoggerFactory.getLogger(DefaultLeaseHonoringSocket.class);

    private volatile Lease currentLease;
    private final LongSupplier currentTimeSupplier;
    private final AtomicInteger remainingQuota;

    @SuppressWarnings("ThrowableInstanceNeverThrown")
    private static final RejectedException rejectedException = new RejectedException("Lease exhausted.");
    private static final Mono<?> rejected = Mono.error(rejectedException);

    public DefaultLeaseHonoringSocket(ReactiveSocket source, LongSupplier currentTimeSupplier) {
        super(source);
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
    public Mono<Void> fireAndForget(Payload payload) {
        return Mono.defer(() -> {
            if (!checkLease()) {
                return rejectError();
            }
            return source.fireAndForget(payload);
        });
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
        return Mono.defer(() -> {
            if (!checkLease()) {
                return rejectError();
            }
            return source.requestResponse(payload);
        });
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
        return Flux.defer(() -> {
            if (!checkLease()) {
                return rejectError();
            }
            return source.requestStream(payload);
        });
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
        return Flux.defer(() -> {
            if (!checkLease()) {
                return rejectError();
            }
            return source.requestChannel(payloads);
        });
    }

    @Override
    public Mono<Void> metadataPush(Payload payload) {
        return Mono.defer(() -> {
            if (!checkLease()) {
                return rejectError();
            }
            return source.metadataPush(payload);
        });
    }

    @Override
    public double availability() {
        return remainingQuota.get() <= 0 || currentLease.isExpired() ? 0.0 : source.availability();
    }

    @SuppressWarnings("unchecked")
    protected <T> Mono<T> rejectError() {
        return (Mono<T>) rejected;
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
