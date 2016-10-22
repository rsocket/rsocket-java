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

package io.reactivesocket.reactivestreams.extensions.internal.processors;

import io.reactivesocket.reactivestreams.extensions.Px;
import io.reactivesocket.reactivestreams.extensions.internal.FlowControlHelper;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * {@link ConnectableUnicastProcessor} is a processor that doesn't start emitting items until it's been subscribed too, and
 * has subscribed to a Publisher. It has a method requestMore that lets you limit the number of items being requested in addition
 * to the items being requested from the subscriber.
 */
public class ConnectableUnicastProcessor<T> implements Processor<T,T>, Px<T> {
    private Subscription subscription;

    private long destinationRequested = 0;
    private long externallyRequested = 0;
    private long actuallyRequested = 0;

    private Subscriber<? super T> destination;

    private boolean complete;
    private boolean erred;
    private boolean cancelled;
    private boolean stated = false;

    private Throwable error;

    private Runnable lazy;

    @Override
    public void subscribe(Subscriber<? super T> destination) {
        this.lazy = () -> {
            if (this.destination != null) {
                destination.onError(new IllegalStateException("Only single Subscriber supported"));

            } else {
                if (error != null) {
                    destination.onError(error);
                    return;
                }
                this.destination = destination;
                destination.onSubscribe(new Subscription() {
                    @Override
                    public void request(long n) {
                        if (canEmit()) {
                            synchronized (ConnectableUnicastProcessor.this) {
                                destinationRequested = FlowControlHelper.incrementRequestN(destinationRequested, n);
                            }
                            tryRequestingMore();
                        }
                    }

                    @Override
                    public void cancel() {
                        synchronized (ConnectableUnicastProcessor.this) {
                            cancelled = true;
                        }
                    }
                });
            }
        };

        start();
    }

    private void start() {
        boolean start = false;
        synchronized (this) {
            if (subscription != null && lazy != null) {
                start = true;
            }
        }

        if (start) {
            lazy.run();
        }
    }

    @Override
    public void onSubscribe(Subscription s) {
        synchronized (this) {
            subscription = s;
        }
        start();

        tryRequestingMore();
    }

    @Override
    public void onNext(T t) {
        if (canEmit()) {
            destination.onNext(t);
        }
    }

    @Override
    public void onError(Throwable t) {
        if (canEmit()) {
            synchronized (this) {
                erred = true;
                error = t;
            }

            destination.onError(t);
        }
    }

    @Override
    public void onComplete() {
        if (canEmit()) {
            synchronized (this) {
                complete = true;
            }
            destination.onComplete();
        }
    }

    private synchronized boolean canEmit() {
        return !complete && !erred && !cancelled;
    }

    public void cancel() {
        synchronized (this) {
            cancelled = true;
        }

        if (subscription != null) {
            subscription.cancel();
        }
    }

    /**
     * Starts the connectable processor with an intial request n
     */
    public void start(long request) {
        synchronized (this) {
            if (stated) {
                return;
            }

            stated = true;
        }
        requestMore(request);
    }

    public void requestMore(long request) {
        if (canEmit()) {
            synchronized (this) {
                externallyRequested = FlowControlHelper.incrementRequestN(externallyRequested, request);
            }
            tryRequestingMore();
        }
    }

    private void tryRequestingMore() {
        long diff;
        synchronized (this) {
            long minRequested = Math.min(externallyRequested, destinationRequested);
            diff = minRequested - actuallyRequested;
            actuallyRequested += diff;
        }
        if (subscription != null && diff > 0) {
            subscription.request(diff);
        }

    }
}
