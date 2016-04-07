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

import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

@FunctionalInterface
public interface ReactiveSocketFactory<T, R extends ReactiveSocket> {

    Publisher<R> call(T t);

    /**
     * Gets a socket in a blocking manner
     * @param t configuration to create the reactive socket
     * @return blocks on create the socket
     */
    default R callAndWait(T t) {
        CompletableFuture<R> future = new CompletableFuture<>();

        call(t)
            .subscribe(new Subscriber<R>() {
                @Override
                public void onSubscribe(Subscription s) {
                    s.request(1);
                }

                @Override
                public void onNext(R reactiveSocket) {
                    future.complete(reactiveSocket);
                }

                @Override
                public void onError(Throwable t) {
                    future.completeExceptionally(t);
                }

                @Override
                public void onComplete() {
                    future.completeExceptionally(new NoSuchElementException("Sequence contains no elements"));
                }
            });

        return future.join();
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
                        if (complete.compareAndSet(false, true)) {
                            subscriber.onComplete();
                        }
                    }
                });

            executorService.schedule(() -> {
                if (complete.compareAndSet(false, true)) {
                    subscriber.onError(new TimeoutException());
                }
            }, timeout, timeUnit);
        };

        return reactiveSocketPublisher;
    }

}
