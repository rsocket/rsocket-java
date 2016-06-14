package io.reactivesocket.loadbalancer.servo;

import com.google.common.util.concurrent.AtomicDouble;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.monitor.DoubleGauge;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.tag.TagList;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.rx.Completable;
import org.reactivestreams.Publisher;

import java.util.function.Consumer;

/**
 * ReactiveSocket that delegates all calls to child reactice socket, and records the current availability as a servo metric
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
    public void start(Completable c) {
        child.start(c);
    }

    @Override
    public void startAndWait() {
        child.startAndWait();
    }

    @Override
    public void onRequestReady(Consumer<Throwable> c) {
        child.onRequestReady(c);
    }

    @Override
    public void onRequestReady(Completable c) {
        child.onRequestReady(c);
    }

    @Override
    public void sendLease(int ttl, int numberOfRequests) {
        child.sendLease(ttl, numberOfRequests);
    }

    @Override
    public void shutdown() {
        child.shutdown();
    }

    @Override
    public void close() throws Exception {
        child.close();
    }

    @Override
    public void onShutdown(Completable c) {
        child.onShutdown(c);
    }
}
