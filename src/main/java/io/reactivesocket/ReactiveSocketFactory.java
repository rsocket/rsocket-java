/**
 * Copyright 2015 Netflix, Inc.
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
package io.reactivesocket;

import io.reactivesocket.internal.rx.EmptySubscription;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@FunctionalInterface
public interface ReactiveSocketFactory<T, R extends ReactiveSocket> {

    Publisher<R> call(T t);

    /**
     * Gets a socket in a blocking manner
     * @param t configuration to create the reactive socket
     * @return blocks on create the socket
     */
    default R callAndWait(T t) {
        AtomicReference<R> reference = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        call(t)
            .subscribe(new Subscriber<R>() {
                @Override
                public void onSubscribe(Subscription s) {
                    s.request(1);
                }

                @Override
                public void onNext(R reactiveSocket) {
                    reference.set(reactiveSocket);
                }

                @Override
                public void onError(Throwable t) {
                    error.set(t);
                    latch.countDown();
                }

                @Override
                public void onComplete() {
                    latch.countDown();
                }
            });

        if (error.get() != null) {
            throw new RuntimeException(error.get());
        } else {
            return reference.get();
        }
    }

    /**
     *
     * @param t the configuration used to create the reactive socket
     * @param timeout timeout
     * @param timeUnit timeout units
     * @param executorService ScheduledExecutorService to schedule the timeout on
     * @return
     */
    default Publisher<R> call(T t, long timeout, TimeUnit timeUnit, ScheduledExecutorService executorService) {
        Publisher<R> reactiveSocketPublisher = subscriber -> {
            AtomicBoolean complete = new AtomicBoolean();
            subscriber.onSubscribe(EmptySubscription.INSTANCE);
            call(t)
                .subscribe(new Subscriber<R>() {
                    @Override
                    public void onSubscribe(Subscription s) {
                        s.request(1);
                    }

                    @Override
                    public void onNext(R reactiveSocket) {
                        subscriber.onNext(reactiveSocket);
                    }

                    @Override
                    public void onError(Throwable t) {
                        subscriber.onError(t);
                    }

                    @Override
                    public void onComplete() {
                        if (!complete.get()) {
                            complete.set(true);
                            subscriber.onComplete();
                        }
                    }
                });

            executorService.schedule(() -> {
                if (!complete.get()) {
                    complete.set(true);
                    subscriber.onError(new TimeoutException());
                }
            }, timeout, timeUnit);
        };

        return reactiveSocketPublisher;
    }

}
