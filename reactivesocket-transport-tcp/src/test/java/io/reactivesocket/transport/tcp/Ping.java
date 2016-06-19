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
package io.reactivesocket.transport.tcp;

import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.transport.tcp.client.TcpReactiveSocketFactory;
import org.HdrHistogram.Recorder;
import rx.Observable;
import rx.RxReactiveStreams;
import rx.Subscriber;
import rx.schedulers.Schedulers;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public final class Ping {

    public static void main(String... args) throws Exception {

        ReactiveSocket reactiveSocket =
                RxReactiveStreams.toObservable(TcpReactiveSocketFactory.create(new InetSocketAddress("localhost", 7878),
                                                                               ConnectionSetupPayload.create("", ""))
                                                                       .apply())
                                 .toSingle()
                                 .toBlocking()
                                 .value();

        byte[] data = "hello".getBytes();

        Payload keyPayload = new Payload() {
            @Override
            public ByteBuffer getData() {
                return ByteBuffer.wrap(data);
            }

            @Override
            public ByteBuffer getMetadata() {
                return null;
            }
        };

        int n = 1_000_000;
        CountDownLatch latch = new CountDownLatch(n);
        final Recorder histogram = new Recorder(3600000000000L, 3);

        Schedulers
            .computation()
            .createWorker()
            .schedulePeriodically(() -> {
                System.out.println("---- PING/ PONG HISTO ----");
                histogram.getIntervalHistogram()
                    .outputPercentileDistribution(System.out, 5, 1000.0, false);
                System.out.println("---- PING/ PONG HISTO ----");
            }, 1, 1, TimeUnit.SECONDS);

        Observable
            .range(1, Integer.MAX_VALUE)
            .flatMap(i -> {
                long start = System.nanoTime();

                return RxReactiveStreams.toObservable(reactiveSocket.requestResponse(keyPayload))
                                        .doOnError(Throwable::printStackTrace)
                                        .doOnNext(s -> {
                                            long diff = System.nanoTime() - start;
                                            histogram.recordValue(diff);
                                        });
            }, 16)
            .doOnError(Throwable::printStackTrace)
            .subscribe(new Subscriber<Payload>() {
                @Override
                public void onCompleted() {

                }

                @Override
                public void onError(Throwable e) {
                    e.printStackTrace();
                }

                @Override
                public void onNext(Payload payload) {
                    latch.countDown();
                }
            });

        latch.await(1, TimeUnit.HOURS);
        System.out.println("Sent => " + n);
        System.exit(0);
    }
}
