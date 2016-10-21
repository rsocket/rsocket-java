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

package io.reactivesocket.reactivestreams.extensions.internal.publishers;

import io.reactivesocket.reactivestreams.extensions.Px;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.CopyOnWriteArrayList;

/**
 *
 */
public class CachingPublisher<T> implements Px<T> {
    private T cached;
    private Throwable error;

    private Publisher<T> source;

    private boolean subscribed;

    private CopyOnWriteArrayList<Subscriber<? super T>> subscribers = new CopyOnWriteArrayList<>();

    public CachingPublisher(Publisher<T> source) {
        this.source = source;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        synchronized (this) {
            if (cached != null || error != null) {
                subscribers.add(s);
                if (!subscribed) {
                    subscribed = true;
                    source
                        .subscribe(new Subscriber<T>() {
                            @Override
                            public void onSubscribe(Subscription s) {
                                s.request(1);
                            }

                            @Override
                            public void onNext(T t) {
                                synchronized (CachingPublisher.this) {
                                    cached = t;
                                    complete();
                                }
                            }

                            @Override
                            public void onError(Throwable t) {
                                synchronized (CachingPublisher.this) {
                                    error = t;
                                    complete();
                                }
                            }

                            void complete() {
                                for (Subscriber s : subscribers) {
                                    if (error != null) {
                                        s.onError(error);
                                    } else {
                                        s.onNext(cached);
                                        s.onComplete();
                                    }
                                }
                            }

                            @Override
                            public void onComplete() {
                                synchronized (CachingPublisher.this) {
                                    if (error == null && cached == null) {
                                        s.onComplete();
                                    }
                                }
                            }
                        });
                }

            } else {
                s.onSubscribe(new Subscription() {
                    boolean cancelled;

                    @Override
                    public synchronized void request(long n) {
                        if (n > 1 && !cancelled) {
                            if (cached == null) {
                                s.onError(error);
                            } else {
                                s.onNext(cached);
                                s.onComplete();
                            }
                        }
                    }

                    @Override
                    public synchronized void cancel() {
                        cancelled = true;
                    }
                });
            }
        }
    }
}
