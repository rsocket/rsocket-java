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
import io.reactivesocket.reactivestreams.extensions.internal.SerializedSubscription;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.function.Function;

public final class SwitchToPublisher<T, R> implements Px<R> {

    private final Px<T> source;
    private final Function<? super T, ? extends Publisher<R>> switchProvider;

    public SwitchToPublisher(Px<T> source, Function<? super T, ? extends Publisher<R>> switchProvider) {
        this.source = source;
        this.switchProvider = switchProvider;
    }

    @Override
    public void subscribe(Subscriber<? super R> target) {
        source.subscribe(new Subscriber<T>() {

            private SerializedSubscription subscription;
            private boolean switched;

            @Override
            public void onSubscribe(Subscription s) {
                synchronized (this) {
                    if (subscription != null) {
                        s.cancel();
                        return;
                    }
                    subscription = new SerializedSubscription(s);
                }
                target.onSubscribe(subscription);
            }

            @Override
            public void onNext(T t) {
                // Do Not notify SerializedSubscription of the item as it is not emitted to the target.
                final boolean switchNow;
                synchronized (this) {
                    switchNow = !switched;
                    switched = true;
                }
                if (switchNow) {
                    subscription.cancelCurrent();
                    Publisher<R> next = switchProvider.apply(t);
                    next.subscribe(new Subscriber<R>() {
                        @Override
                        public void onSubscribe(Subscription s) {
                            subscription.replaceSubscription(s);
                        }

                        @Override
                        public void onNext(R r) {
                            subscription.onItemReceived();
                            target.onNext(r);
                        }

                        @Override
                        public void onError(Throwable t) {
                            target.onError(t);
                        }

                        @Override
                        public void onComplete() {
                            target.onComplete();
                        }
                    });
                }
            }

            @Override
            public void onError(Throwable t) {
                boolean _switched;
                synchronized (this) {
                    _switched = switched;
                    if (!switched) {
                        switched = true;// Handle cases when onNext arrives after terminate.
                    }
                }
                if (!_switched) {
                    // Empty source completes the target subscriber.
                    target.onError(t);
                }
            }

            @Override
            public void onComplete() {
                boolean _switched;
                synchronized (this) {
                    _switched = switched;
                    if (!switched) {
                        switched = true;// Handle cases when onNext arrives after complete.
                    }
                }
                if (!_switched) {
                    // Empty source completes the target subscriber.
                    target.onComplete();
                }
            }
        });
    }
}
