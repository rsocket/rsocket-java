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
package io.reactivesocket.aeron.example.requestreply;

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

                System.out.println("---- PING/ PONG HISTO ----");
                histogram.getIntervalHistogram().outputPercentileDistribution(System.out, 5, 1000.0, false);
                System.out.println("---- PING/ PONG HISTO ----");


            }, 10, 10, TimeUnit.SECONDS);

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
                        reactiveSocket
                            .requestResponse(keyPayload))
                    .doOnNext(s -> {
                        long diff = System.nanoTime() - start;
                        histogram.recordValue(diff);
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
