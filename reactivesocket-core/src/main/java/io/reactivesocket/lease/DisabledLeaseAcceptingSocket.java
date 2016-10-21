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
import org.reactivestreams.Publisher;

import java.util.function.Consumer;

public final class DisabledLeaseAcceptingSocket implements LeaseEnforcingSocket {

    private final ReactiveSocket delegate;

    public DisabledLeaseAcceptingSocket(ReactiveSocket delegate) {
        this.delegate = delegate;
    }

    @Override
    public void acceptLeaseSender(Consumer<Lease> leaseSender) {
        // No Op, shouldn't be used when leases are required.
    }

    @Override
    public Publisher<Void> fireAndForget(Payload payload) {
        return delegate.fireAndForget(payload);
    }

    @Override
    public Publisher<Payload> requestResponse(Payload payload) {
        return delegate.requestResponse(payload);
    }

    @Override
    public Publisher<Payload> requestStream(Payload payload) {
        return delegate.requestStream(payload);
    }

    @Override
    public Publisher<Payload> requestSubscription(
            Payload payload) {
        return delegate.requestSubscription(payload);
    }

    @Override
    public Publisher<Payload> requestChannel(
            Publisher<Payload> payloads) {
        return delegate.requestChannel(payloads);
    }

    @Override
    public Publisher<Void> metadataPush(Payload payload) {
        return delegate.metadataPush(payload);
    }

    @Override
    public double availability() {
        return delegate.availability();
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
