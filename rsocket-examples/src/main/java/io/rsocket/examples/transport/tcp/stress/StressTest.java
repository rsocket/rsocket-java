/*
 * Copyright 2016 Netflix, Inc.
 * <p>
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *  <p>
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  <p>
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations under the License.
 */

package io.rsocket.examples.transport.tcp.stress;

import io.rsocket.RSocket;
import io.rsocket.client.LoadBalancingClient;
import io.rsocket.exceptions.RejectedException;
import io.rsocket.server.RSocketServer;
import io.rsocket.transport.netty.server.TcpServerTransport;
import org.HdrHistogram.Recorder;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.ipc.netty.tcp.TcpServer;

import java.net.SocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

class StressTest {

    private final AtomicInteger serverCount = new AtomicInteger(0);
    private final TestConfig config;
    private final AtomicInteger successes;
    private final AtomicInteger failures;
    private final AtomicInteger leaseExhausted;
    private final AtomicInteger timeouts;
    private final AtomicInteger outstandings = new AtomicInteger();
    private final Recorder histogram;
    private volatile long testStartTime;
    private RSocket clientSocket;
    private Disposable printDisposable;

    StressTest(TestConfig config) {
        this.config = config;
        successes = new AtomicInteger(0);
        failures = new AtomicInteger(0);
        leaseExhausted = new AtomicInteger();
        timeouts = new AtomicInteger();
        histogram = new Recorder(TimeUnit.MINUTES.toNanos(1), 4);
    }

    public StressTest printStatsEvery(Duration duration) {
        printDisposable = Flux.interval(duration)
                .doOnEach(aLong -> {
                    printTestStats(false);
                }).subscribe();
        return this;
    }

    public void printTestStats(boolean printLatencyDistribution) {
        System.out.println("==============================================================");
        long timeElapsed = (System.nanoTime() - testStartTime) / 1_000_000;
        System.out.println(successes.get() + " events in " + timeElapsed +
                           " ms. Test time remaining(ms): " + (config.getTestDuration().toMillis() - timeElapsed));
        double rps = 1_000_000_000.0 * successes.get() / (System.nanoTime() - testStartTime);
        System.out.println(rps + " rps");
        double rate = (double) successes.get() / (successes.get() + failures.get());
        System.out.println("successes: " + successes.get()
                           + ", failures: " + failures.get()
                           + ", timeouts: " + timeouts.get()
                           + ", lease exhaustion: " + leaseExhausted.get()
                           + ", success rate: " + rate);
        if (printLatencyDistribution) {
            System.out.println("Latency distribution in us");
            histogram.getIntervalHistogram().outputPercentileDistribution(System.out, 1000.0);
        }
        System.out.println("==============================================================");
        System.out.flush();
    }

    public StressTest startClient() {
        LoadBalancingClient client = LoadBalancingClient.create(getServerList(),
                                                                address -> config.newClientForServer(address));
        clientSocket = client.connect().block();
        System.out.println("Client ready!");
        return this;
    }

    private Publisher<? extends Collection<SocketAddress>> getServerList() {
        return config.serverListChangeTicks()
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

    public void startTest(Function<RSocket, Publisher<?>> testFunction) {
        if (clientSocket == null) {
            System.err.println("Client not connected. Call startClient() first.");
            System.exit(-1);
        }
        testStartTime = System.nanoTime();
        while (System.nanoTime() - testStartTime < config.getTestDuration().toNanos()) {
            if (outstandings.get() <= config.getMaxConcurrency()) {
                AtomicLong startTime = new AtomicLong();
                Flux.defer(() -> testFunction.apply(clientSocket))
                        .doOnSubscribe(subscription -> {
                            startTime.set(System.nanoTime());
                            outstandings.incrementAndGet();
                        })
                        .doAfterTerminate(() -> {
                            long elapsed = (System.nanoTime() - startTime.get()) / 1000;
                            histogram.recordValue(elapsed);
                            outstandings.decrementAndGet();
                        })
                        .doOnComplete(() -> {
                            successes.incrementAndGet();
                        })
                        .onErrorResumeWith(e -> {
                            failures.incrementAndGet();
                            if (e instanceof RejectedException) {
                                leaseExhausted.incrementAndGet();
                            } else if (e instanceof TimeoutException) {
                                timeouts.incrementAndGet();
                            }
                            if (failures.get() % 1000 == 0) {
                                e.printStackTrace();
                            }
                            return Flux.empty();
                        })
                        .subscribe();
            } else {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    System.out.println("Interrupted while waiting for lowering concurrency.");
                    Thread.currentThread().interrupt();
                }
            }
        }
        System.out.println("Stress test finished. Duration (minutes): "
                           + Duration.ofNanos(System.nanoTime() - testStartTime).toMinutes());
        printTestStats(true);
        clientSocket.close().block();

        if (null != printDisposable) {
            printDisposable.dispose();
        }
    }

    private SocketAddress startServer() {
        return RSocketServer.create(TcpServerTransport.create(TcpServer.create()))
                                   .start((setup, sendingSocket) -> {
                                       return config.nextServerHandler(serverCount.incrementAndGet());
                                   })
                                   .getServerAddress();
    }
}
