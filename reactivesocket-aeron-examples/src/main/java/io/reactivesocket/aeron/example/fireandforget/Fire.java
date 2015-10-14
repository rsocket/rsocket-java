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
package io.reactivesocket.aeron.example.fireandforget;


import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.aeron.client.AeronClientDuplexConnection;
import io.reactivesocket.aeron.client.AeronClientDuplexConnectionFactory;
import io.reactivesocket.aeron.client.FrameHolder;
import org.HdrHistogram.Recorder;
import org.reactivestreams.Publisher;
import rx.Observable;
import rx.RxReactiveStreams;
import rx.Subscriber;
import rx.schedulers.Schedulers;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Fire {
    public static void main(String... args) throws Exception {
        String host = System.getProperty("host", "localhost");
        String server = System.getProperty("server", "localhost");

        System.out.println("Setting host to => " + host);

        System.out.println("Setting ping is listening to => " + server);


        byte[] payload = new byte[40];
        Random r = new Random();
        r.nextBytes(payload);

        System.out.println("Sending data of size => " + payload.length);

        InetSocketAddress listenAddress = new InetSocketAddress("localhost", 39790);
        InetSocketAddress clientAddress = new InetSocketAddress("localhost", 39790);

        AeronClientDuplexConnectionFactory cf = AeronClientDuplexConnectionFactory.getInstance();
        cf.addSocketAddressToHandleResponses(listenAddress);
        Publisher<AeronClientDuplexConnection> udpConnection = cf.createAeronClientDuplexConnection(clientAddress);

        System.out.println("Creating new duplex connection");
        AeronClientDuplexConnection connection = RxReactiveStreams.toObservable(udpConnection).toBlocking().single();
        System.out.println("Created duplex connection");

        ReactiveSocket reactiveSocket = ReactiveSocket.fromClientConnection(connection, ConnectionSetupPayload.create("UTF-8", "UTF-8", ConnectionSetupPayload.NO_FLAGS));
        reactiveSocket.startAndWait();

        CountDownLatch latch = new CountDownLatch(Integer.MAX_VALUE);

        final Recorder histogram = new Recorder(3600000000000L, 3);

        Schedulers
            .computation()
            .createWorker()
            .schedulePeriodically(() -> {
                System.out.println("---- FRAME HOLDER HISTO ----");
                FrameHolder.histogram.getIntervalHistogram().outputPercentileDistribution(System.out, 5, 1000.0, false);
                System.out.println("---- FRAME HOLDER HISTO ----");

                System.out.println("---- Fire / Forget HISTO ----");
                histogram.getIntervalHistogram().outputPercentileDistribution(System.out, 5, 1000.0, false);
                System.out.println("---- Fire / Forget HISTO ----");


            }, 10, 10, TimeUnit.SECONDS);

        Observable
            .range(1, Integer.MAX_VALUE)
            .flatMap(i -> {
                long start = System.nanoTime();

                Payload keyPayload = new Payload() {
                    ByteBuffer data = ByteBuffer.wrap(payload);
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
                        reactiveSocket
                            .fireAndForget(keyPayload))
                    .finallyDo(() -> {
                        long diff = System.nanoTime() - start;
                        histogram.recordValue(diff);
                    });
            })
            .subscribe(new Subscriber<Void>() {
                @Override
                public void onCompleted() {

                }

                @Override
                public void onError(Throwable e) {
                    e.printStackTrace();
                }

                @Override
                public void onNext(Void v) {
                    latch.countDown();
                }
            });

        latch.await();
        System.out.println("Sent => " + Integer.MAX_VALUE);
        System.exit(0);
    }

}
