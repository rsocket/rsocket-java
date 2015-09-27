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
package io.reactivesocket.aeron.client;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import io.reactivesocket.Payload;
import rx.Observable;
import rx.RxReactiveStreams;
import rx.Subscriber;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by rroeser on 8/16/15.
 */
public class Ping {

    public static void main(String... args) throws Exception {
        String host = System.getProperty("host", "localhost");
        String server = System.getProperty("server", "localhost");

        System.out.println("Setting host to => " + host);

        System.out.println("Setting ping is listening to => " + server);


        byte[] key = new byte[4];
        //byte[] key = new byte[BitUtil.SIZE_OF_INT];
        Random r = new Random();
        r.nextBytes(key);

        System.out.println("Sending data of size => " + key.length);

        final MetricRegistry metrics = new MetricRegistry();
        final Timer timer = metrics.timer("pingTimer");

        final ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MICROSECONDS)
            .build();
        reporter.start(15, TimeUnit.SECONDS);

        ReactiveSocketAeronClient client = ReactiveSocketAeronClient.create(host, server);

        CountDownLatch latch = new CountDownLatch(Integer.MAX_VALUE);


        Observable
            .range(1, Integer.MAX_VALUE)
            .flatMap(i -> {
                long start = System.nanoTime();

                Payload keyPayload = new Payload() {
                    ByteBuffer data = ByteBuffer.wrap(key);
                    ByteBuffer metadata = ByteBuffer.allocate(0);

                    public ByteBuffer getData() {
                        return data;
                    }

                    @Override
                    public ByteBuffer getMetadata() {
                        return metadata;
                    }
                };

                return RxReactiveStreams
                    .toObservable(
                        client
                            .requestResponse(keyPayload))
                    .doOnNext(s -> {
                        long diff = System.nanoTime() - start;
                        timer.update(diff, TimeUnit.NANOSECONDS);
                    });
            })
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

        latch.await();
        System.out.println("Sent => " + Integer.MAX_VALUE);
        System.exit(0);
    }

}
