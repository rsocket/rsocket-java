package io.reactivesocket.internal;

import io.reactivesocket.Payload;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class LimitableRequestPublisher<T extends Payload> extends Flux<T> implements Subscription {
    private final Publisher<T> source;

    private final AtomicBoolean canceled;

    private long internalRequested;

    private long externalRequested;

    private volatile boolean subscribed;

    private volatile Subscription s;

    private LimitableRequestPublisher(Publisher<T> source) {
        this.source = source;
        this.canceled = new AtomicBoolean();
    }

    public static <T extends Payload> LimitableRequestPublisher<T> wrap(Publisher<T> source) {
        return new LimitableRequestPublisher<>(source);
    }

    @Override
    public void subscribe(Subscriber<? super T> destination) {
        synchronized (this) {
            if (subscribed) {
                throw new IllegalStateException("only one subscriber at a time");
            }

            subscribed = true;
        }

        destination.onSubscribe(new InnerSubscription());
        source.subscribe(new InnerSubscriber<>(destination));

        if (source instanceof Fuseable.ScalarCallable) {
            Fuseable.ScalarCallable source = (Fuseable.ScalarCallable) this.source;
            Object call = source.call();
            destination.onNext((T) call);
            destination.onComplete();
        }
    }

    public void increaseRequestLimit(long n) {
        synchronized (this) {
            externalRequested = Operators.addCap(n, externalRequested);
        }

        requestN();
    }

    @Override
    public void request(long n) {
        increaseRequestLimit(n);
    }

    private void requestN() {
        long r;
        synchronized (this) {
            if (s == null) {
                return;
            }

            r = Math.min(internalRequested, externalRequested);
            externalRequested -= r;
            internalRequested -= r;
        }

        if (r > 0) {
            s.request(r);
        }
    }

    public void cancel() {
        if (canceled.compareAndSet(false, true) && s != null) {
            s.cancel();
            s = null;
            subscribed = false;
        }
    }

    private class InnerSubscriber<T> implements Subscriber<T> {
        Subscriber<? super T> destination;

        public InnerSubscriber(Subscriber<? super T> destination) {
            this.destination = destination;
        }

        @Override
        public void onSubscribe(Subscription s) {
            synchronized (LimitableRequestPublisher.this) {
                LimitableRequestPublisher.this.s = s;

                if (canceled.get()) {
                    s.cancel();
                    subscribed = false;
                    LimitableRequestPublisher.this.s = null;
                }
            }
        }

        @Override
        public void onNext(T t) {
            destination.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            destination.onError(t);
        }

        @Override
        public void onComplete() {
            destination.onComplete();
        }
    }

    private class InnerSubscription implements Subscription {

        @Override
        public void request(long n) {
            synchronized (LimitableRequestPublisher.this) {
                internalRequested = Operators.addCap(n, internalRequested);
            }

            requestN();
        }

        @Override
        public void cancel() {
            LimitableRequestPublisher.this.cancel();
        }
    }

}
