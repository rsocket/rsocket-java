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

import io.reactivesocket.reactivestreams.extensions.internal.SerializedSubscription;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import io.reactivesocket.reactivestreams.extensions.Px;

public final class ConcatPublisher<T> implements Px<T> {

    private final Publisher<T> first;
    private final Publisher<T> second;

    public ConcatPublisher(Publisher<T> first, Publisher<T> second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public void subscribe(Subscriber<? super T> destination) {
        first.subscribe(new Subscriber<T>() {
                    private SerializedSubscription subscription;

                    @Override
                    public void onSubscribe(Subscription s) {
                        subscription = new SerializedSubscription(s);
                        destination.onSubscribe(subscription);
                    }

                    @Override
                    public void onNext(T t) {
                        subscription.onItemReceived();
                        destination.onNext(t);
                    }

                    @Override
                    public void onError(Throwable t) {
                        destination.onError(t);
                    }

                    @Override
                    public void onComplete() {
                        second.subscribe(new Subscriber<T>() {
                            @Override
                            public void onSubscribe(Subscription s) {
                                subscription.replaceSubscription(s);
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
                        });
                    }
                });
    }
}
