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

package io.reactivesocket.spectator;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.impl.AtomicDouble;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import org.reactivestreams.Publisher;

/**
 * ReactiveSocket that delegates all calls to child reactive socket, and records the current availability as a servo metric
 */
public class AvailabilityMetricReactiveSocket implements ReactiveSocket {

    private final ReactiveSocket child;
    private final AtomicDouble atomicDouble;

    public AvailabilityMetricReactiveSocket(ReactiveSocket child, Registry registry, String name, String monitorId) {
        atomicDouble = new AtomicDouble();
        this.child = child;
        Id id = registry.createId(name, "id", monitorId);
        registry.gauge(id, this, socket -> socket.atomicDouble.get());
    }

    @Override
    public Publisher<Payload> requestResponse(Payload payload) {
        return child.requestResponse(payload);
    }

    @Override
    public Publisher<Void> fireAndForget(Payload payload) {
        return child.fireAndForget(payload);
    }

    @Override
    public Publisher<Payload> requestStream(Payload payload) {
        return child.requestStream(payload);
    }

    @Override
    public Publisher<Payload> requestSubscription(Payload payload) {
        return child.requestSubscription(payload);
    }

    @Override
    public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
        return child.requestChannel(payloads);
    }

    @Override
    public Publisher<Void> metadataPush(Payload payload) {
        return child.metadataPush(payload);
    }

    @Override
    public double availability() {
        double availability = child.availability();
        atomicDouble.set(availability);
        return availability;
    }

    @Override
    public Publisher<Void> close() {
        return child.close();
    }

    @Override
    public Publisher<Void> onClose() {
        return child.onClose();
    }
}
