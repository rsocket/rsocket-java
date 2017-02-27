package io.reactivesocket.internal;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.MonoSource;
import reactor.core.publisher.Operators;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

public class MonoOnErrorOrCancelReturn<T> extends MonoSource<T, T> {
    final Function<? super Throwable, ? extends T> onError;
    final Supplier<? extends T> onCancel;

    public MonoOnErrorOrCancelReturn(Publisher<T> source, Function<? super Throwable, ? extends T> onError, Supplier<? extends T> onCancel) {
        super(source);
        this.onError = Objects.requireNonNull(onError, "onError");
        this.onCancel = Objects.requireNonNull(onCancel, "onCancel");
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new OnErrorOrCancelReturnSubscriber<T>(s, onError, onCancel));
    }

    static final class OnErrorOrCancelReturnSubscriber<T> extends Operators.MonoSubscriber<T, T> {
        final Function<? super Throwable, ? extends T> onError;
        final Supplier<? extends T> onCancel;

        Subscription s;

        int count;

        boolean done;

        public OnErrorOrCancelReturnSubscriber(Subscriber<? super T> actual, Function<? super Throwable, ? extends T> onError, Supplier<? extends T> onCancel) {
            super(actual);
            this.onError = onError;
            this.onCancel = onCancel;
        }

        @Override
        public void request(long n) {
            super.request(n);
            if (n > 0L) {
                s.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void cancel() {
            s.cancel();
            complete(onCancel.get());
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (Operators.validate(this.s, s)) {
                this.s = s;
                actual.onSubscribe(this);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onNext(T t) {
            if (done) {
                Operators.onNextDropped(t);
                return;
            }
            value = t;

            if (++count > 1) {
                cancel();

                onError(new IndexOutOfBoundsException("Source emitted more than one item"));
            }
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                Operators.onErrorDropped(t);
                return;
            }
            done = true;

            complete(onError.apply(t));
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;

            int c = count;
            if (c == 0) {
                actual.onError(Operators.onOperatorError(this,
                        new NoSuchElementException("Source was empty")));
            } else if (c == 1) {
                complete(value);
            }
        }
    }
}