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
package io.reactivesocket.examples;

import io.reactivesocket.ConnectionSetupHandler;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.RequestHandler;
import io.reactivesocket.client.ClientBuilder;
import io.reactivesocket.transport.tcp.client.TcpReactiveSocketConnector;
import io.reactivesocket.util.Unsafe;
import io.reactivesocket.test.TestUtil;
import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import io.reactivesocket.transport.tcp.server.TcpReactiveSocketServer;
import rx.Observable;
import rx.RxReactiveStreams;
import rx.functions.Func1;

public class StressTest {
    private static SocketAddress startServer() throws InterruptedException {
        boolean bad = ThreadLocalRandom.current().nextInt(4) == 0; // 25% of bad servers

        ConnectionSetupHandler setupHandler = (setupPayload, reactiveSocket) ->
            new RequestHandler.Builder()
                .withRequestResponse(
                    payload ->
                        subscriber -> {
                            Subscription subscription = new Subscription() {
                                @Override
                                public void request(long n) {
                                    if (bad) {
                                        if (ThreadLocalRandom.current().nextInt(3) == 0) {
                                            subscriber.onError(new Exception("SERVER EXCEPTION"));
                                        } else {
                                            // This will generate a timeout
                                            //System.out.println("Server: No response");
                                        }
                                    } else {
                                        subscriber.onNext(TestUtil.utf8EncodedPayload("RESPONSE", "NO_META"));
                                        subscriber.onComplete();
                                    }
                                }

                                @Override
                                public void cancel() {}
                            };
                            subscriber.onSubscribe(subscription);
                        }
                )
                .build();

        SocketAddress addr = new InetSocketAddress("127.0.0.1", 0);
        TcpReactiveSocketServer.StartedServer server = TcpReactiveSocketServer.create(addr).start(setupHandler);
        SocketAddress serverAddress = server.getServerAddress();

        System.out.println("Server started at " + serverAddress);
        return serverAddress;
    }

    private static Publisher<List<SocketAddress>> getServersList() {
        Observable<List<SocketAddress>> serverAddresses = Observable.interval(1, TimeUnit.SECONDS)
            .map(new Func1<Long, List<SocketAddress>>() {
                List<SocketAddress> addresses = new ArrayList<>();

                @Override
                public List<SocketAddress> call(Long aLong) {
                    try {
                        SocketAddress socketAddress = startServer();
                        System.out.println("Adding server " + socketAddress);
                        addresses.add(socketAddress);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    if (addresses.size() > 15) {
                        SocketAddress address = addresses.get(0);
                        System.out.println("Removing server " + address);
                        addresses.remove(address);
                    }
                    return new ArrayList<>(addresses);
                }
            });
        return RxReactiveStreams.toPublisher(serverAddresses);
    }

    public static void main(String... args) throws Exception {
        ConnectionSetupPayload setupPayload =
            ConnectionSetupPayload.create("UTF-8", "UTF-8", ConnectionSetupPayload.HONOR_LEASE);

        TcpReactiveSocketConnector tcp = TcpReactiveSocketConnector.create(setupPayload, Throwable::printStackTrace);

        ReactiveSocket client = ClientBuilder.instance()
            .withSource(getServersList())
            .withConnector(tcp)
            .withConnectTimeout(1, TimeUnit.SECONDS)
            .withRequestTimeout(1, TimeUnit.SECONDS)
            .build();

        Unsafe.awaitAvailability(client);
        System.out.println("Client ready, starting the load...");

        long testDurationNs = TimeUnit.NANOSECONDS.convert(30, TimeUnit.SECONDS);
        AtomicInteger successes = new AtomicInteger(0);
        AtomicInteger failures = new AtomicInteger(0);

        long start = System.nanoTime();
        ConcurrentHistogram histogram = new ConcurrentHistogram(TimeUnit.MINUTES.toNanos(1), 4);
        histogram.setAutoResize(true);

        int concurrency = 100;
        AtomicInteger outstandings = new AtomicInteger(0);
        while (System.nanoTime() - start < testDurationNs) {
            if (outstandings.get() <= concurrency) {
                Payload request = TestUtil.utf8EncodedPayload("Hello", "META");
                client.requestResponse(request).subscribe(new MeasurerSusbcriber<>(histogram, successes, failures, outstandings));
            } else {
                Thread.sleep(1);
            }
        }

        Thread.sleep(1000);
        System.out.println(successes.get() + " events in " + (System.nanoTime() - start) / 1_000_000 + " ms");
        double rps = (1_000_000_000.0 * successes.get())/(System.nanoTime() - start);
        System.out.println(rps + " rps");
        double rate = ((double) successes.get()) / (successes.get() + failures.get());
        System.out.println("successes: " + successes.get()
            + ", failures: " + failures.get()
            + ", success rate: " + rate);
        System.out.println("Latency distribution in us");
        histogram.outputPercentileDistribution(System.out, 1000.0);
        System.out.flush();
    }

    private static class MeasurerSusbcriber<T> implements Subscriber<T> {
        private final Histogram histo;
        private final AtomicInteger successes;
        private final AtomicInteger failures;
        private AtomicInteger outstandings;
        private long start;

        private MeasurerSusbcriber(
            Histogram histo,
            AtomicInteger successes,
            AtomicInteger failures,
            AtomicInteger outstandings
        ) {
            this.histo = histo;
            this.successes = successes;
            this.failures = failures;
            this.outstandings = outstandings;
        }

        @Override
        public void onSubscribe(Subscription s) {
            start = System.nanoTime();
            outstandings.incrementAndGet();
            s.request(1L);
        }

        @Override
        public void onNext(T t) {}

        @Override
        public void onError(Throwable t) {
            record();
            System.err.println("Error: " + t);
            failures.incrementAndGet();
        }

        @Override
        public void onComplete() {
            record();
            successes.incrementAndGet();
        }

        private void record() {
            long elapsed = (System.nanoTime() - start) / 1000;
            histo.recordValue(elapsed);
            outstandings.decrementAndGet();
        }
    }
}
