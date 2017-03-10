package io.reactivesocket.internal;

import io.reactivesocket.Payload;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.core.publisher.Flux;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class LimitableRequestPublisher<T extends Payload> extends Flux<T> {
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
        long e;
        long i;
        synchronized (this) {
            e = FlowControlHelper.incrementRequestN(externalRequested, n);
            externalRequested = e;
            i = internalRequested;
        }

        requestUpstream(e, i);
    }

    private void requestUpstream(long e, long i) {
        long n = Math.min(e , i);

        if (n > 0 && s != null & !canceled.get()) {
            s.request(n);
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
            synchronized (LimitableRequestPublisher.this) {
                externalRequested--;
                internalRequested--;
            }

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
            long e;
            long i;
            synchronized (this) {
                i = FlowControlHelper.incrementRequestN(internalRequested, n);
                internalRequested = i;
                e = externalRequested;
            }

            requestUpstream(e, i);
        }

        @Override
        public void cancel() {
            LimitableRequestPublisher.this.cancel();
        }
    }
}
