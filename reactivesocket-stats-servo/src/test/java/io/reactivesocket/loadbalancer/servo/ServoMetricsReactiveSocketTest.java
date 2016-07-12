/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket.loadbalancer.servo;

import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.internal.Publishers;
import io.reactivesocket.internal.rx.EmptySubscription;
import io.reactivesocket.rx.Completable;
import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import rx.RxReactiveStreams;
import rx.observers.TestSubscriber;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

/**
 * Created by rroeser on 3/7/16.
 */
public class ServoMetricsReactiveSocketTest {
    @Test
    public void testCountSuccess() {
        ServoMetricsReactiveSocket client = new ServoMetricsReactiveSocket(new ReactiveSocket() {
            @Override
            public Publisher<Void> metadataPush(Payload payload) {
                return null;
            }

            @Override
            public Publisher<Void> fireAndForget(Payload payload) {
                return null;
            }

            @Override
            public Publisher<Payload> requestSubscription(Payload payload) {
                return null;
            }

            @Override
            public Publisher<Payload> requestStream(Payload payload) {
                return null;
            }

            @Override
            public Publisher<Payload> requestResponse(Payload payload) {
                return s -> {
                    s.onNext(new Payload() {
                        @Override
                        public ByteBuffer getData() {
                            return null;
                        }

                        @Override
                        public ByteBuffer getMetadata() {
                            return null;
                        }
                    });

                    s.onComplete();
                };
            }

            @Override
            public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
                return null;
            }

            @Override
            public double availability() {
                return 1.0;
            }

            @Override
            public Publisher<Void> close() {
                return Publishers.empty();
            }

            @Override
            public Publisher<Void> onClose() {
                return Publishers.empty();
            }

            @Override
            public void start(Completable completable) {}
            @Override
            public void onRequestReady(Consumer<Throwable> consumer) {}
            @Override
            public void onRequestReady(Completable completable) {}
            @Override
            public void sendLease(int i, int i1) {}
        }, "test");

        Publisher<Payload> payloadPublisher = client.requestResponse(new Payload() {
            @Override
            public ByteBuffer getData() {
                return null;
            }

            @Override
            public ByteBuffer getMetadata() {
                return null;
            }
        });

        TestSubscriber<Payload> subscriber = new TestSubscriber<>();
        RxReactiveStreams.toObservable(payloadPublisher).subscribe(subscriber);
        subscriber.awaitTerminalEvent();
        subscriber.assertNoErrors();

        Assert.assertEquals(1, client.success.get());
    }

    @Test
    public void testCountFailure() {
        ServoMetricsReactiveSocket client = new ServoMetricsReactiveSocket(new ReactiveSocket() {
            @Override
            public Publisher<Void> metadataPush(Payload payload) {
                return null;
            }

            @Override
            public Publisher<Void> fireAndForget(Payload payload) {
                return null;
            }

            @Override
            public Publisher<Payload> requestSubscription(Payload payload) {
                return null;
            }

            @Override
            public Publisher<Payload> requestStream(Payload payload) {
                return null;
            }

            @Override
            public Publisher<Payload> requestResponse(Payload payload) {
                return new Publisher<Payload>() {
                    @Override
                    public void subscribe(Subscriber<? super Payload> s) {
                        s.onSubscribe(EmptySubscription.INSTANCE);
                        s.onError(new RuntimeException());
                    }
                };
            }

            @Override
            public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
                return null;
            }

            @Override
            public double availability() {
                return 1.0;
            }

            @Override
            public Publisher<Void> close() {
                return Publishers.empty();
            }

            @Override
            public Publisher<Void> onClose() {
                return Publishers.empty();
            }

            @Override
            public void start(Completable completable) {}
            @Override
            public void onRequestReady(Consumer<Throwable> consumer) {}
            @Override
            public void onRequestReady(Completable completable) {}
            @Override
            public void sendLease(int i, int i1) {}
        }, "test");

        Publisher<Payload> payloadPublisher = client.requestResponse(new Payload() {
            @Override
            public ByteBuffer getData() {
                return null;
            }

            @Override
            public ByteBuffer getMetadata() {
                return null;
            }
        });

        TestSubscriber<Payload> subscriber = new TestSubscriber<>();
        RxReactiveStreams.toObservable(payloadPublisher).subscribe(subscriber);
        subscriber.awaitTerminalEvent();
        subscriber.assertError(RuntimeException.class);

        Assert.assertEquals(1, client.failure.get());

    }

    @Test
    public void testHistogram() throws Exception {
        ServoMetricsReactiveSocket client = new ServoMetricsReactiveSocket(new ReactiveSocket() {
            @Override
            public Publisher<Void> metadataPush(Payload payload) {
                return null;
            }

            @Override
            public Publisher<Void> fireAndForget(Payload payload) {
                return null;
            }

            @Override
            public Publisher<Payload> requestSubscription(Payload payload) {
                return null;
            }

            @Override
            public Publisher<Payload> requestStream(Payload payload) {
                return null;
            }

            @Override
            public Publisher<Payload> requestResponse(Payload payload) {
                try {
                    Thread.sleep(ThreadLocalRandom.current().nextInt(10, 50));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return s -> {
                    s.onSubscribe(EmptySubscription.INSTANCE);
                    s.onNext(new Payload() {
                        @Override
                        public ByteBuffer getData() {
                            return null;
                        }

                        @Override
                        public ByteBuffer getMetadata() {
                            return null;
                        }
                    });

                    s.onComplete();
                };
            }

            @Override
            public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
                return null;
            }

            @Override
            public double availability() {
                return 1.0;
            }

            @Override
            public Publisher<Void> close() {
                return Publishers.empty();
            }

            @Override
            public Publisher<Void> onClose() {
                return Publishers.empty();
            }

            @Override
            public void start(Completable completable) {}
            @Override
            public void onRequestReady(Consumer<Throwable> consumer) {}
            @Override
            public void onRequestReady(Completable completable) {}
            @Override
            public void sendLease(int i, int i1) {}
        }, "test");

        for (int i = 0; i < 10; i ++) {
            Publisher<Payload> payloadPublisher = client.requestResponse(new Payload() {
                @Override
                public ByteBuffer getData() {
                    return null;
                }

                @Override
                public ByteBuffer getMetadata() {
                    return null;
                }
            });

            TestSubscriber<Payload> subscriber = new TestSubscriber<>();
            RxReactiveStreams.toObservable(payloadPublisher).subscribe(subscriber);
            subscriber.awaitTerminalEvent();
            subscriber.assertNoErrors();
        }

        Thread.sleep(3_000);

        System.out.println(client.histrogramToString());

        Assert.assertEquals(10, client.success.get());
        Assert.assertEquals(0, client.failure.get());
        Assert.assertNotEquals(client.timer.getMax(), client.timer.getMin());
    }
}
