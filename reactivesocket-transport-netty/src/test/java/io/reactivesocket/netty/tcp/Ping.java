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
package io.reactivesocket.netty.tcp;

import io.netty.channel.nio.NioEventLoopGroup;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.DefaultReactiveSocket;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.netty.tcp.client.ClientTcpDuplexConnection;
import org.HdrHistogram.Recorder;
import org.reactivestreams.Publisher;
import rx.Observable;
import rx.RxReactiveStreams;
import rx.Subscriber;
import rx.schedulers.Schedulers;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Ping {
    public static void main(String... args) throws Exception {
        Publisher<ClientTcpDuplexConnection> publisher = ClientTcpDuplexConnection
            .create(InetSocketAddress.createUnresolved("localhost", 7878), new NioEventLoopGroup(1));

        ClientTcpDuplexConnection duplexConnection = RxReactiveStreams.toObservable(publisher).toBlocking().last();
        ReactiveSocket reactiveSocket = DefaultReactiveSocket.fromClientConnection(duplexConnection, ConnectionSetupPayload.create("UTF-8", "UTF-8"), t -> t.printStackTrace());

        reactiveSocket.startAndWait();

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

                return RxReactiveStreams
                    .toObservable(
                        reactiveSocket
                            .requestResponse(keyPayload))
                    .doOnError(t -> t.printStackTrace())
                    .doOnNext(s -> {
                        long diff = System.nanoTime() - start;
                        histogram.recordValue(diff);
                    });
            }, 16)
            .doOnError(t -> t.printStackTrace())
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
