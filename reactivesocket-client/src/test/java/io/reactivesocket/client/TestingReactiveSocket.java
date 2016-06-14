package io.reactivesocket.client;

import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.internal.rx.EmptySubscription;
import io.reactivesocket.rx.Completable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

public class TestingReactiveSocket implements ReactiveSocket {
    private final Function<Payload, Payload> responder;
    private final AtomicInteger count;

    public TestingReactiveSocket(Function<Payload, Payload> responder) {
        this.responder = responder;
        this.count = new AtomicInteger(0);
    }

    public int countMessageReceived() {
        return count.get();
    }

    @Override
    public Publisher<Void> fireAndForget(Payload payload) {
        return subscriber -> {
            subscriber.onSubscribe(EmptySubscription.INSTANCE);
            subscriber.onNext(null);
        };
    }

    @Override
    public Publisher<Payload> requestResponse(Payload payload) {
        return subscriber ->
            subscriber.onSubscribe(new Subscription() {
                boolean cancelled = false;

                @Override
                public void request(long n) {
                    if (cancelled) {
                        return;
                    }
                    try {
                        count.incrementAndGet();
                        Payload response = responder.apply(payload);
                        subscriber.onNext(response);
                        subscriber.onComplete();
                    } catch (Throwable t) {
                        subscriber.onError(t);
                    }
                }

                @Override
                public void cancel() {}
            });
    }

    @Override
    public Publisher<Payload> requestStream(Payload payload) {
        return requestResponse(payload);
    }

    @Override
    public Publisher<Payload> requestSubscription(Payload payload) {
        return requestResponse(payload);
    }

    @Override
    public Publisher<Payload> requestChannel(Publisher<Payload> inputs) {
        return subscriber ->
            inputs.subscribe(new Subscriber<Payload>() {
                @Override
                public void onSubscribe(Subscription s) {
                    subscriber.onSubscribe(s);
                }

                @Override
                public void onNext(Payload input) {
                    Payload response = responder.apply(input);
                    subscriber.onNext(response);
                }

                @Override
                public void onError(Throwable t) {
                    subscriber.onError(t);
                }

                @Override
                public void onComplete() {
                    subscriber.onComplete();
                }
            });
    }

    @Override
    public Publisher<Void> metadataPush(Payload payload) {
        return fireAndForget(payload);
    }

    @Override
    public double availability() {
        return 1.0;
    }

    @Override
    public void start(Completable c) {
        c.success();
    }

    @Override
    public void onRequestReady(Consumer<Throwable> c) {}

    @Override
    public void onRequestReady(Completable c) {
        c.success();
    }

    @Override
    public void onShutdown(Completable c) {}

    @Override
    public void sendLease(int ttl, int numberOfRequests) {
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public void shutdown() {}

    @Override
    public void close() throws Exception {}
}
