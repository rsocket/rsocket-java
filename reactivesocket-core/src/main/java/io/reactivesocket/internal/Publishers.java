/*
 * Copyright 2016 Netflix, Inc.
 * <p>
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *  <p>
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  <p>
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations under the License.
 */

package io.reactivesocket.internal;

import io.reactivesocket.exceptions.TimeoutException;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * A set of utility functions for applying function composition over {@link Publisher}s.
 */
public final class Publishers {

    @SuppressWarnings("ThrowableInstanceNeverThrown")
    public static final TimeoutException TIMEOUT_EXCEPTION = new TimeoutException();

    private Publishers() {
        // No instances.
    }

    /**
     * Converts a {@code Publisher} of type {@code T} to a {@code Publisher} of type {@code R} using the passed
     * {@code map} function.
     *
     * @param source {@code Publisher} to map.
     * @param map {@code Function} to use for conversion.
     *
     * @param <T> Type of source {@code Publisher}.
     * @param <R> Type of resulting {@code Publisher}.
     *
     * @return A new {@code Publisher} which takes objects of type {@code R} instead of {@code T}.
     */
    public static <T, R> Publisher<R> map(Publisher<T> source, Function<T, R> map) {
        return subscriber -> {
            source.subscribe(new MapSubscriber<>(subscriber, map));
        };
    }

    /**
     * Adds a timeout for the first emission from the {@code source}. If the source emits multiple items then this
     * timeout does <em>not</em> apply for further emissions.
     *
     * @param source Source to apply the timeout on.
     * @param timeoutSignal Source after termination of which, {@code source} will be cancelled, if not already done.
     *
     * @param <T> Type of items emitted by {@code source}.
     *
     * @return A new {@code Publisher} with timeout applied.
     */
    public static <T> Publisher<T> timeout(Publisher<T> source, Publisher<Void> timeoutSignal) {
        return s -> {
            source.subscribe(new SafeCancellableSubscriberProxy<T>(s) {

                private Runnable timeoutCancellation;
                private boolean emitted;

                @Override
                public void onSubscribe(Subscription s) {
                    timeoutCancellation = afterTerminate(timeoutSignal, () -> {
                        boolean _cancel = true;
                        synchronized (this) {
                            _cancel = !emitted;
                        }
                        if (_cancel) {
                            onError(TIMEOUT_EXCEPTION);
                            cancel();
                        }
                    });
                    super.onSubscribe(s);
                }

                @Override
                protected void doOnNext(T t) {
                    synchronized (this) {
                        emitted = true;
                    }
                    timeoutCancellation.run(); // Cancel the timeout since we have received one item.
                    super.doOnNext(t);
                }

                @Override
                protected void doOnError(Throwable t) {
                    timeoutCancellation.run();
                    super.doOnError(t);
                }

                @Override
                protected void doOnComplete() {
                    timeoutCancellation.run();
                    super.doOnComplete();
                }

                @Override
                protected void doAfterCancel() {
                    timeoutCancellation.run();
                }
            });
        };
    }

    /**
     * Creates a new {@code Publisher} that completes after the passed {@code interval} passes.
     *
     * @param scheduler Scheduler to use for scheduling the interval.
     * @param interval Interval after which the timer ticks.
     * @param timeUnit Unit for the interval.
     *
     * @return new {@code Publisher} that completes after the interval passes.
     */
    public static Publisher<Void> timer(ScheduledExecutorService scheduler, long interval, TimeUnit timeUnit) {
        return s -> {
            scheduler.schedule(() -> s.onComplete(), interval, timeUnit);
        };
    }

    /**
     * Concats {@code first} source with the {@code second} source. This will subscribe to the {@code second} source
     * when the first one completes. Any errors from the {@code first} source will result in not subscribing to the
     * {@code second} source
     *
     * @param first source to subscribe.
     * @param second source to subscribe.
     *
     * @return New {@code Publisher} which concats both the passed sources.
     */
    public static Publisher<Void> concatEmpty(Publisher<Void> first, Publisher<Void> second) {
        return subscriber -> {
            first.subscribe(new SafeCancellableSubscriberProxy<Void>(subscriber) {
                @Override
                protected void doOnComplete() {
                    second.subscribe(new SafeCancellableSubscriber<Void>() {
                        @Override
                        public void onSubscribe(Subscription s) {
                            super.onSubscribe(s);
                            // This is the second subscription which isn't driven by downstream subscriber.
                            // So, no onSubscriber callback will be coming here (alread done for first subscriber).
                            // As we are only dealing with empty (Void) sources, this doesn't break backpressure.
                            s.request(1);
                        }

                        @Override
                        protected void doOnNext(Void aVoid) {
                            subscriber.onNext(aVoid);
                        }

                        @Override
                        protected void doOnError(Throwable t) {
                            subscriber.onError(t);
                        }

                        @Override
                        protected void doOnComplete() {
                            subscriber.onComplete();
                        }
                    });
                }
            });
        };
    }

    /**
     * A new {@code Publisher} that just emits the passed error on subscription.
     *
     * @param error that the returned source will emit.
     *
     * @return New {@code Publisher} which emits the passed {@code error}.
     */
    public static <T> Publisher<T> error(Throwable error) {
        return subscriber -> {
            subscriber.onSubscribe(new SingleEmissionSubscription<T>(subscriber, error));
        };
    }

    /**
     * A new {@code Publisher} that just emits the passed {@code item} and completes.
     *
     * @param item that the returned source will emit.
     *
     * @return New {@code Publisher} which just emits the passed {@code item}.
     */
    public static <T> Publisher<T> just(T item) {
        return subscriber -> {
            subscriber.onSubscribe(new SingleEmissionSubscription<T>(subscriber, item));
        };
    }

    /**
     * A new {@code Publisher} that immediately completes without emitting any item.
     *
     * @return New {@code Publisher} which immediately completes without emitting any item.
     */
    public static <T> Publisher<T> empty() {
        return subscriber -> {
            subscriber.onSubscribe(new SingleEmissionSubscription<T>(subscriber));
        };
    }

    /**
     * Subscribes to the passed source and invokes the {@code action} once after either {@link Subscriber#onComplete()}
     * or {@link Subscriber#onError(Throwable)} is invoked.
     *
     * @param source Source to subscribe.
     * @param action Action to invoke on termination.
     *
     * @return Cancellation handle.
     */
    public static Runnable afterTerminate(Publisher<Void> source, Runnable action) {
        final CancellableSubscriber<Void> subscriber = new SafeCancellableSubscriber<Void>() {
            @Override
            public void doOnError(Throwable t) {
                action.run();
            }

            @Override
            public void doOnComplete() {
                action.run();
            }
        };
        source.subscribe(subscriber);
        return () -> subscriber.cancel();
    }

    private static class MapSubscriber<T, R> extends SafeCancellableSubscriber<T> {
        private final Subscriber<? super R> subscriber;
        private final Function<T, R> map;

        public MapSubscriber(Subscriber<? super R> subscriber, Function<T, R> map) {
            this.subscriber = subscriber;
            this.map = map;
        }

        @Override
        public void onSubscribe(Subscription s) {
            Subscription s1 = new Subscription() {
                @Override
                public void request(long n) {
                    s.request(n);
                }

                @Override
                public void cancel() {
                    MapSubscriber.this.cancel();
                }
            };
            super.onSubscribe(s1);
            subscriber.onSubscribe(s1);
        }

        @Override
        protected void doOnNext(T t) {
            try {
                R r = map.apply(t);
                subscriber.onNext(r);
            } catch (Exception e) {
                onError(e);
            }
        }

        @Override
        protected void doOnError(Throwable t) {
            subscriber.onError(t);
        }

        @Override
        protected void doOnComplete() {
            subscriber.onComplete();
        }
    }
}
