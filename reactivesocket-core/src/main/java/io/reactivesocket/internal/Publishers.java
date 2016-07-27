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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.reactivesocket.internal.CancellableSubscriberImpl.*;

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
            source.subscribe(Subscribers.create(subscription -> {
                subscriber.onSubscribe(subscription);
            }, t -> {
                R r = map.apply(t);
                subscriber.onNext(r);
            }, throwable -> {
                subscriber.onError(throwable);
            }, () -> subscriber.onComplete(), EMPTY_RUNNABLE));
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
            final AtomicReference<Runnable> timeoutCancellation = new AtomicReference<>();
            CancellableSubscriber<T> sub = Subscribers.create(
                    subscription -> {
                        timeoutCancellation.set(afterTerminate(timeoutSignal, () -> {
                            s.onError(TIMEOUT_EXCEPTION);
                        }));
                        s.onSubscribe(subscription);
                    },
                    t -> {
                        timeoutCancellation.get().run();
                        s.onNext(t);
                    },
                    throwable -> {
                        timeoutCancellation.get().run();
                        s.onError(throwable);
                    },
                    () -> {
                        timeoutCancellation.get().run();
                        s.onComplete();
                    }, () -> {
                        timeoutCancellation.get().run();
                    });
            source.subscribe(sub);
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
            first.subscribe(Subscribers.create(subscription -> {
                subscriber.onSubscribe(subscription);
            }, t -> {
                subscriber.onNext(t);
            }, throwable -> {
                subscriber.onError(throwable);
            }, () -> {
                second.subscribe(Subscribers.create(subscription -> {
                    // This is the second subscription which isn't driven by downstream subscriber.
                    // So, no onSubscriber callback will be coming here (alread done for first subscriber).
                    // As we are only dealing with empty (Void) sources, this doesn't break backpressure.
                    subscription.request(1);
                }, t -> {
                    subscriber.onNext(t);
                }, throwable -> {
                    subscriber.onError(throwable);
                }, () -> {
                    subscriber.onComplete();
                }, EMPTY_RUNNABLE));
            }, EMPTY_RUNNABLE));
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
        final CancellableSubscriber<Void> subscriber = Subscribers.doOnTerminate(throwable -> action.run(),
                                                                                 () -> action.run());
        source.subscribe(subscriber);
        return () -> subscriber.cancel();
    }

    private static final class TimeoutHolder<T> implements Consumer<Subscription>, Runnable {

        private final Publisher<Void> timeoutSignal;
        private final Subscriber<? super T> subscriber;
        private Runnable timeoutCancellation;

        private TimeoutHolder(Publisher<Void> timeoutSignal, Subscriber<? super T> subscriber) {
            this.timeoutSignal = timeoutSignal;
            this.subscriber = subscriber;
        }

        @Override
        public void run() {
            timeoutCancellation.run();
        }

        @Override
        public void accept(Subscription subscription) {
            timeoutCancellation = afterTerminate(timeoutSignal, () -> {
                subscriber.onError(TIMEOUT_EXCEPTION);
            });
        }
    }
}
