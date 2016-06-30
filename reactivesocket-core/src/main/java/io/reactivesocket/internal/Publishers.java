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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * A set of utility functions for applying function composition over {@link Publisher}s.
 */
public final class Publishers {

    @SuppressWarnings("ThrowableInstanceNeverThrown")
    public static final TimeoutException TIMEOUT_EXCEPTION = new TimeoutException();

    public static final StackTraceElement[] EMPTY_STACK = new StackTraceElement[0];

    static {
        TIMEOUT_EXCEPTION.setStackTrace(EMPTY_STACK);
    }

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
            source.subscribe(new Subscriber<T>() {
                @Override
                public void onSubscribe(Subscription s) {
                    subscriber.onSubscribe(s);
                }

                @Override
                public void onNext(T t) {
                    try {
                        R r = map.apply(t);
                        subscriber.onNext(r);
                    } catch (Exception e) {
                        onError(e);
                    }
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
     * Adds retrying on errors to the passed {@code source}.
     *
     * @param source Source to add retry to.
     * @param retryCount Number of times to retry.
     * @param retrySelector Function that determines whether an error is retryable.
     *
     * @param <T> Type of items emitted by the source.
     *
     * @return A new {@code Publisher} with retry enabled.
     */
    public static <T> Publisher<T> retry(Publisher<T> source, int retryCount,
                                         Function<Throwable, Boolean> retrySelector) {
        return s -> {
            source.subscribe(new SafeCancellableSubscriberProxy<T>(s) {
                private final AtomicInteger budget = new AtomicInteger(retryCount);
                @Override
                protected void doOnError(Throwable t) {
                    if (retrySelector.apply(t) && budget.decrementAndGet() >= 0) {
                        done.set(false); // Reset done since we subscribe again.
                        // Since cancellation flag isn't cleared, if the subscription cancelled then this new
                        // subscription will automatically be cancelled.
                        source.subscribe(this);
                    } else {
                        super.doOnError(t); // Proxy to delegate.
                    }
                }
            });
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

    private static abstract class SafeCancellableSubscriberProxy<T> extends SafeCancellableSubscriber<T> {

        private final Subscriber<? super T> delegate;

        protected SafeCancellableSubscriberProxy(Subscriber<? super T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void onSubscribe(Subscription s) {
            super.onSubscribe(s);
            delegate.onSubscribe(s);
        }

        @Override
        protected void doOnNext(T t) {
            delegate.onNext(t);
        }

        @Override
        protected void doOnError(Throwable t) {
            delegate.onError(t);
        }

        @Override
        protected void doOnComplete() {
            delegate.onComplete();
        }
    }

    private static abstract class SafeCancellableSubscriber<T> extends CancellableSubscriber<T> {

        protected final AtomicBoolean done = new AtomicBoolean();

        @Override
        public void onNext(T t) {
            if (!done.get()) {
                doOnNext(t);
            }
        }

        @Override
        public void onError(Throwable t) {
            if (done.compareAndSet(false, true)) {
                doOnError(t);
            }
        }

        @Override
        public void onComplete() {
            if (done.compareAndSet(false, true)) {
                doOnComplete();
            }
        }

        @Override
        public void cancel() {
            if (done.compareAndSet(false, true)) {
                super.cancel();
            }
        }

        protected void doOnNext(T t) {
            // NoOp by default
        }

        protected void doOnError(Throwable t) {
            // NoOp by default
        }

        protected void doOnComplete() {
            // NoOp by default
        }
    }

    private static abstract class CancellableSubscriber<T> implements Subscriber<T> {

        private Subscription s;
        private boolean cancelled;

        @Override
        public void onSubscribe(Subscription s) {
            boolean _cancel = false;
            synchronized (this) {
                this.s = s;
                if (cancelled) {
                    _cancel = true;
                }
            }

            if (_cancel) {
                _unsafeCancel();
            }
        }

        public void cancel() {
            boolean _cancel = false;
            synchronized (this) {
                cancelled = true;
                if (s != null) {
                    _cancel = true;
                }
            }

            if (_cancel) {
                _unsafeCancel();
            }
        }

        protected void doAfterCancel() {
            // NoOp by default.
        }

        private void _unsafeCancel() {
            s.cancel();
            doAfterCancel();
        }
    }
}
