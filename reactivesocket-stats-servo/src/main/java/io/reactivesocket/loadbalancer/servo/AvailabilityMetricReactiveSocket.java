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

package io.reactivesocket.loadbalancer.servo;

import com.google.common.util.concurrent.AtomicDouble;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.monitor.DoubleGauge;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.tag.TagList;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import org.reactivestreams.Publisher;

/**
 * ReactiveSocket that delegates all calls to child reactive socket, and records the current availability as a servo metric
 */
public class AvailabilityMetricReactiveSocket implements ReactiveSocket {
    private final ReactiveSocket child;

    private final DoubleGauge availabilityGauge;

    private final AtomicDouble atomicDouble;

    public AvailabilityMetricReactiveSocket(ReactiveSocket child, String name, TagList tags) {
        this.child = child;
        MonitorConfig.Builder builder = MonitorConfig.builder(name);

        if (tags != null) {
            builder.withTags(tags);
        }
        MonitorConfig config = builder.build();
        availabilityGauge = new DoubleGauge(config);
        DefaultMonitorRegistry.getInstance().register(availabilityGauge);
        atomicDouble = availabilityGauge.getNumber();
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
