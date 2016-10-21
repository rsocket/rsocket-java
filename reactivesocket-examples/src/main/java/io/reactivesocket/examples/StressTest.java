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
package io.reactivesocket.examples;

import io.reactivesocket.AbstractReactiveSocket;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.client.KeepAliveProvider;
import io.reactivesocket.client.LoadBalancingClient;
import io.reactivesocket.client.ReactiveSocketClient;
import io.reactivesocket.client.SetupProvider;
import io.reactivesocket.lease.DisabledLeaseAcceptingSocket;
import io.reactivesocket.lease.FairLeaseDistributor;
import io.reactivesocket.reactivestreams.extensions.ExecutorServiceBasedScheduler;
import io.reactivesocket.server.ReactiveSocketServer;
import io.reactivesocket.transport.tcp.client.TcpTransportClient;
import io.reactivesocket.transport.tcp.server.TcpTransportServer;
import io.reactivesocket.util.PayloadImpl;
import io.reactivex.Flowable;
import io.reactivex.functions.Function;
import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.reactivesocket.client.filter.ReactiveSocketClients.*;
import static io.reactivesocket.client.filter.ReactiveSockets.*;

public final class StressTest {

    private static final AtomicInteger count = new AtomicInteger(0);

    private static SocketAddress startServer() {
        // 25% of bad servers
        boolean bad = count.incrementAndGet() % 4 == 3;
        FairLeaseDistributor leaseDistributor = new FairLeaseDistributor(() -> 5000, 5000,
                                                                         Flowable.interval(0, 30, TimeUnit.SECONDS));
        return ReactiveSocketServer.create(TcpTransportServer.create())
                                   .start((setup, sendingSocket) -> {
                                       return new DisabledLeaseAcceptingSocket(new AbstractReactiveSocket() {
                                           @Override
                                           public Publisher<Payload> requestResponse(Payload payload) {
                                               return Flowable.defer(() -> {
                                                   if (bad) {
                                                       if (ThreadLocalRandom.current().nextInt(2) == 0) {
                                                           return Flowable.error(new Exception("SERVER EXCEPTION"));
                                                       } else {
                                                           return Flowable.never(); // Cause timeout
                                                       }
                                                   } else {
                                                       return Flowable.just(new PayloadImpl("Response"));
                                                   }
                                               });
                                           }
                                       });
                                   })
                                   .getServerAddress();
    }

    private static Publisher<Collection<SocketAddress>> getServersList() {
        return Flowable.interval(2, TimeUnit.SECONDS)
                   .map(aLong -> startServer())
                   .map(new Function<SocketAddress, Collection<SocketAddress>>() {
                       private final List<SocketAddress> addresses = new ArrayList<SocketAddress>();

                       @Override
                       public Collection<SocketAddress> apply(SocketAddress socketAddress) {
                           System.out.println("Adding server " + socketAddress);
                           addresses.add(socketAddress);
                           if (addresses.size() > 15) {
                               SocketAddress address = addresses.remove(0);
                               System.out.println("Removed server " + address);
                           }
                           return addresses;
                       }
                   });
    }

    public static void main(String... args) throws Exception {
        long testDurationNs = TimeUnit.NANOSECONDS.convert(60, TimeUnit.SECONDS);

        ExecutorServiceBasedScheduler scheduler = new ExecutorServiceBasedScheduler();
        SetupProvider setupProvider = SetupProvider.keepAlive(KeepAliveProvider.never()).disableLease();
        LoadBalancingClient client = LoadBalancingClient.create(getServersList(),
                                 address -> {
                                     TcpTransportClient transport = TcpTransportClient.create(address);
                                     ReactiveSocketClient raw = ReactiveSocketClient.create(transport, setupProvider);
                                     return wrap(detectFailures(connectTimeout(raw, 1, TimeUnit.SECONDS, scheduler)),
                                                 timeout(1, TimeUnit.SECONDS, scheduler));
                                 });

        ReactiveSocket socket = Flowable.fromPublisher(client.connect())
                                      .switchIfEmpty(Flowable.error(new IllegalStateException("No socket returned.")))
                                      .blockingFirst();

        System.out.println("Client ready, starting the load...");


        AtomicInteger successes = new AtomicInteger(0);
        AtomicInteger failures = new AtomicInteger(0);

        long start = System.nanoTime();
        ConcurrentHistogram histogram = new ConcurrentHistogram(TimeUnit.MINUTES.toNanos(1), 4);
        histogram.setAutoResize(true);

        int concurrency = 100;
        AtomicInteger outstandings = new AtomicInteger(0);
        while (System.nanoTime() - start < testDurationNs) {
            if (outstandings.get() <= concurrency) {
                Payload request = new PayloadImpl("Hello", "META");
                socket.requestResponse(request)
                      .subscribe(new MeasurerSusbcriber<>(histogram, successes, failures, outstandings));
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
        Flowable.fromPublisher(socket.close()).ignoreElements().blockingGet();
        System.exit(-1);
    }

    private static class MeasurerSusbcriber<T> implements Subscriber<T> {
        private final Histogram histo;
        private final AtomicInteger successes;
        private final AtomicInteger failures;
        private final AtomicInteger outstandings;
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
            failures.incrementAndGet();
            if (failures.get() % 1000 == 0) {
                System.err.println("Error: " + t);
            }
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
