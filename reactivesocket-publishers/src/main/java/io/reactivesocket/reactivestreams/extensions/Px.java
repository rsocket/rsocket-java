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

package io.reactivesocket.reactivestreams.extensions;

import io.reactivesocket.reactivestreams.extensions.internal.ValidatingSubscription;
import io.reactivesocket.reactivestreams.extensions.internal.publishers.CachingPublisher;
import io.reactivesocket.reactivestreams.extensions.internal.publishers.ConcatPublisher;
import io.reactivesocket.reactivestreams.extensions.internal.publishers.DoOnEventPublisher;
import io.reactivesocket.reactivestreams.extensions.internal.publishers.SwitchToPublisher;
import io.reactivesocket.reactivestreams.extensions.internal.publishers.TimeoutPublisher;
import io.reactivesocket.reactivestreams.extensions.internal.subscribers.Subscribers;
import org.agrona.UnsafeAccess;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

/**
 * Extension of the {@link Publisher} interface with useful methods to create and transform data
 *
 * @param <T>
 */
public interface Px<T> extends Publisher<T> {

    /**
     * A new {@code Px} that just emits the passed {@code item} and completes.
     *
     * @param item that the returned source will emit.
     * @return New {@code Publisher} which just emits the passed {@code item}.
     */
    static <T> Px<T> just(T item) {
        return subscriber ->
            subscriber.onSubscribe(new Subscription() {
                boolean cancelled;

                @Override
                public void request(long n) {
                    if (isCancelled()) {
                        return;
                    }

                    if (n < 0) {
                        subscriber.onError(new IllegalArgumentException("n less than zero"));
                    }

                    try {
                        subscriber.onNext(item);
                        subscriber.onComplete();
                    } catch (Throwable t) {
                        subscriber.onError(t);
                    }
                }

                private boolean isCancelled() {
                    UnsafeAccess.UNSAFE.loadFence();
                    return cancelled;
                }

                @Override
                public void cancel() {
                    cancelled = true;
                    UnsafeAccess.UNSAFE.storeFence();
                }
            });
    }

    /**
     * A new {@code Px} that does not emit any item, just completes. The returned {@code Px} instance
     * does not wait for {@link Subscription#request(long)} invocations before emitting the completion.
     *
     * @return New {@code Publisher} which just completes upon subscription.
     */
    static <T> Px<T> empty() {
        return subscriber -> {
            try {
                subscriber.onSubscribe(EMPTY_SUBSCRIPTION);
                subscriber.onComplete();
            } catch (Throwable t) {
                subscriber.onError(t);
            }
        };
    }

    /**
     * Creates a new Px for you and passes in a subscriber
     * @param subscriberConsumer closure that access a subscriber
     * @param <T>
     * @return a new Px instance
     */
    static <T> Px<T> create(Consumer<Subscriber<? super T>> subscriberConsumer) {
        return subscriberConsumer::accept;
    }

    @FunctionalInterface
    interface OnComplete extends Runnable {
    }

    Subscription EMPTY_SUBSCRIPTION = new Subscription() {
        @Override
        public void request(long n) {
            if (n < 0) {
                throw new IllegalArgumentException("n must be greater than zero");
            }

        }

        @Override
        public void cancel() {
        }
    };

    /**
     * A new {@code Px} that does not emit any item and never terminates.
     *
     * @return New {@code Publisher} which does not emit any item and never terminates.
     */
    static <T> Px<T> never() {
        return subscriber -> {
            try {
                subscriber.onSubscribe(ValidatingSubscription.empty(subscriber));
            } catch (Throwable t) {
                subscriber.onError(t);
            }
        };
    }

    /**
     * A new {@code Px} that does not emit any item, just emits the passed {@code t}. The returned {@code Px} instance
     * does not wait for {@link Subscription#request(long)} invocations before emitting the error.
     *
     * @return New {@code Publisher} which just completes upon subscription.
     */
    static <T> Px<T> error(Throwable t) {
        return s -> {
            s.onSubscribe(ValidatingSubscription.empty(s));
            s.onError(t);
        };
    }

    /**
     * Converts the passed {@code source} to an instance of {@code Px}. <p>
     * Returns the same object if {@code source} is already an instance of {@code Px}
     *
     * @param source {@code Publisher} to convert.
     * @param <T>    Type for the source {@code Publisher}
     * @return Source converted to {@code Px}
     */
    static <T> Px<T> from(Publisher<T> source) {
        if (source instanceof Px) {
            return (Px<T>) source;
        } else {
            return source::subscribe;
        }
    }

    /**
     * A new {@code Px} that does not emit any items, it just calls the {@link Runnable}
     * passed to it. It either completes, or errors but doesn't emit an item. This will
     * not emit until a {@link Subscription#request(long)} invocation. It will not
     * emit if it is cancelled.
     * <p>
     * If you receive an Exception convert it to a {@link RuntimeException} and throw it
     * and it will be handle properly.
     *
     * @param onComplete called by your callback to single when it's complete
     * @return New {@code Publisher} which completes when a Runnable is executed.
     */
    static Px<Void> completable(Consumer<OnComplete> onComplete) {
        return subscriber -> {
            try {
                subscriber.onSubscribe(EMPTY_SUBSCRIPTION);
                onComplete.accept(subscriber::onComplete);
            } catch (Throwable t) {
                subscriber.onError(t);
            }

        };
    }

    /**
     * Converts an eager {@code Publisher} to a lazy {@code Publisher}.
     *
     * @param supplierSource Supplier for the eager {@code Publisher}.
     * @param <T>            Type of the source.
     * @return Lazy {@code Publisher} delegating to the supplied source.
     */
    static <T> Px<T> defer(Supplier<Publisher<T>> supplierSource) {
        return s -> {
            Publisher<T> src = supplierSource.get();
            if (src == null) {
                src = error(new NullPointerException("Deferred Publisher is null."));
            }
            src.subscribe(s);
        };
    }

    /**
     * Concats two empty ({@code Void}) {@code Publisher}s into a single {@code Publisher}.
     *
     * @param first  First {@code Publisher} to subscribe.
     * @param second Second {@code Publisher} to subscribe only if the first did not terminate with an error.
     * @return A new {@code Px} that concats the two passed {@code Publisher}s.
     */
    static Px<Void> concatEmpty(Publisher<Void> first, Publisher<Void> second) {
        return subscriber -> {
            first.subscribe(Subscribers.create(subscriber::onSubscribe, subscriber::onNext, subscriber::onError, () -> {
                second.subscribe(Subscribers.create(subscription -> {
                    // This is the second subscription which isn't driven by downstream subscriber.
                    // So, no onSubscriber callback will be coming here (alread done for first subscriber).
                    // As we are only dealing with empty (Void) sources, this doesn't break backpressure.
                    subscription.request(1);
                }, subscriber::onNext, subscriber::onError, subscriber::onComplete, null));
            }, null));
        };
    }

    /**
     * A special {@link #map(Function)} that blindly casts all items emitted from this {@code Px} to the passed class.
     *
     * @param toClass Target class to cast.
     * @param <R>     Type after the cast.
     * @return A new {@code Px} of the target type.
     */
    default <R> Px<R> cast(Class<R> toClass) {
        return (Px<R>) this;
    }

    /**
     * On emission of the first item from this {@code Px} switches to a new {@code Publisher} as provided by the
     * {@code mapper}. Resulting {@code Px} will cancel the subscription to this {@code Px} after the emission of the
     * first item and subscribe to the new {@code Publisher} returned by the {@code mapper}.
     *
     * @param mapper Function that provides the <em>next</em> {@code Publisher}.
     * @param <R>    Type of the resulting {@code Px}.
     * @return A new {@code Px} that switches to a new {@code Publisher} on emission of the first item.
     */
    default <R> Px<R> switchTo(Function<? super T, ? extends Publisher<R>> mapper) {
        return new SwitchToPublisher<T, R>(this, mapper);
    }

    default <R> Px<R> map(Function<? super T, ? extends R> mapper) {
        return subscriber ->
            subscribe(new Subscriber<T>() {
                volatile boolean canEmit = true;

                @Override
                public void onSubscribe(Subscription s) {
                    subscriber.onSubscribe(s);
                }

                @Override
                public void onNext(T t) {
                    if (canEmit) {
                        try {
                            R apply = mapper.apply(t);
                            subscriber.onNext(apply);
                        } catch (Throwable e) {
                            onError(e);
                        }
                    }
                }

                @Override
                public void onError(Throwable t) {
                    if (canEmit) {
                        canEmit = false;
                        subscriber.onError(t);
                    }
                }

                @Override
                public void onComplete() {
                    if (canEmit) {
                        canEmit = false;
                        subscriber.onComplete();
                    }
                }
            });
    }

    /**
     * Returns a publisher that ignores onNexts, and only emits onComplete, and onErrors.
     */
    default Px<Void> ignore() {
        return subscriber ->
            subscribe(new Subscriber<T>() {
                Subscription subscription;

                @Override
                public void onSubscribe(Subscription s) {
                    subscription = s;
                    subscriber.onSubscribe(s);
                }

                @Override
                public void onNext(T t) {
                }

                @Override
                public void onError(Throwable t) {
                    subscription.cancel();
                    subscriber.onError(t);
                }

                @Override
                public void onComplete() {
                    subscriber.onComplete();
                }
            });
    }

    /**
     * Caches the first item emitted and completes
     *
     * @return Publishers that caches one item
     */
    default Px<T> cacheOne() {
        return new CachingPublisher<>(this);
    }

    default Px<T> emitOnCancel(Supplier<T> onCancel) {
        return emitOnCancelOrError(onCancel, null);
    }

    default Px<T> emitOnCancelOrError(Supplier<T> onCancel, Function<Throwable, T> onError) {
        return subscriber ->
            subscriber.onSubscribe(new Subscription() {
                boolean started;
                Subscription inner;
                boolean cancelled;

                @Override
                public void request(long n) {
                    if (n < 0) {
                        subscriber.onError(new IllegalStateException("n is less than zero"));
                    }

                    boolean start = false;
                    synchronized (this) {
                        if (!started) {
                            started = true;
                            start = true;
                        }
                    }

                    if (start) {
                        subscribe(new Subscriber<T>() {
                            @Override
                            public void onSubscribe(Subscription s) {
                                inner = s;
                            }

                            @Override
                            public void onNext(T t) {
                                subscriber.onNext(t);
                            }

                            @Override
                            public void onError(Throwable t) {
                                synchronized (this) {
                                    if (cancelled) {
                                        return;
                                    }
                                }

                                if (onError != null) {
                                    T apply = onError.apply(t);
                                    subscriber.onNext(apply);
                                    subscriber.onComplete();
                                    return;
                                }

                                subscriber.onError(t);
                            }

                            @Override
                            public void onComplete() {
                                subscriber.onComplete();
                            }
                        });
                    }

                    if (inner != null) {
                        inner.request(n);
                    }
                }

                @Override
                public void cancel() {
                    synchronized (this) {
                        if (cancelled) {
                            return;
                        }

                        cancelled = true;
                    }
                    if (inner != null) {
                        inner.cancel();
                    }

                    subscriber.onNext(onCancel.get());
                    subscriber.onComplete();
                }
            });
    }

    default Px<T> doOnSubscribe(Consumer<Subscription> doOnSubscribe) {
        return DoOnEventPublisher.onSubscribe(this, doOnSubscribe);
    }

    default Px<T> doOnRequest(LongConsumer doOnRequest) {
        return DoOnEventPublisher.onRequest(this, doOnRequest);
    }

    default Px<T> doOnCancel(Runnable doOnCancel) {
        return DoOnEventPublisher.onCancel(this, doOnCancel);
    }

    default Px<T> doOnNext(Consumer<T> doOnNext) {
        return DoOnEventPublisher.onNext(this, doOnNext);
    }

    default Px<T> doOnError(Consumer<Throwable> doOnError) {
        return DoOnEventPublisher.onError(this, doOnError);
    }

    default Px<T> doOnComplete(Runnable doOnComplete) {
        return DoOnEventPublisher.onComplete(this, doOnComplete);
    }

    default Px<T> doOnCompleteOrError(Runnable doOnComplete, Consumer<Throwable> doOnError) {
        return DoOnEventPublisher.onCompleteOrError(this, doOnComplete, doOnError);
    }

    default Px<T> doOnTerminate(Runnable doOnTerminate) {
        return DoOnEventPublisher.onTerminate(this, doOnTerminate);
    }

    default Px<T> timeout(long timeout, TimeUnit unit, Scheduler scheduler) {
        return new TimeoutPublisher<>(this, timeout, unit, scheduler);
    }

    default Px<T> concatWith(Publisher<T> concatSource) {
        return new ConcatPublisher<>(this, concatSource);
    }

    @SuppressWarnings("unchecked")
    default void subscribe() {
        subscribe(DefaultSubscriber.defaultInstance());
    }

}
