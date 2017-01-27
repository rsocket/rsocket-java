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

package io.reactivesocket.examples.transport.tcp.stress;

import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.client.KeepAliveProvider;
import io.reactivesocket.client.ReactiveSocketClient;
import io.reactivesocket.client.SetupProvider;
import io.reactivesocket.lease.DefaultLeaseEnforcingSocket;
import io.reactivesocket.lease.DisabledLeaseAcceptingSocket;
import io.reactivesocket.lease.FairLeaseDistributor;
import io.reactivesocket.lease.LeaseEnforcingSocket;
import io.reactivesocket.reactivestreams.extensions.ExecutorServiceBasedScheduler;
import io.reactivesocket.transport.tcp.client.TcpTransportClient;
import io.reactivex.Flowable;

import java.net.SocketAddress;
import java.time.Duration;
import java.util.function.IntSupplier;

import static io.reactivesocket.client.filter.ReactiveSocketClients.*;
import static io.reactivesocket.client.filter.ReactiveSockets.*;
import static io.reactivesocket.examples.transport.tcp.stress.StressTestHandler.*;
import static java.util.concurrent.TimeUnit.*;

public class TestConfig {

    private final Duration testDuration;
    private final int maxConcurrency;
    private final IntSupplier serverCapacitySupplier;
    private final int leaseTtlMillis;
    private final SetupProvider setupProvider;
    private static final ExecutorServiceBasedScheduler scheduler = new ExecutorServiceBasedScheduler();

    public TestConfig() {
        this(Duration.ofMinutes(1), 100, true);
    }

    public TestConfig(Duration testDuration, int maxConcurrency) {
        this(testDuration, maxConcurrency, true);
    }

    public TestConfig(Duration testDuration) {
        this(testDuration, 100, true);
    }

    public TestConfig(Duration testDuration, int maxConcurrency, boolean enableLease) {
        this(testDuration, maxConcurrency, enableLease? () -> Integer.MAX_VALUE : null, 30_000);
    }

    public TestConfig(Duration testDuration, int maxConcurrency, IntSupplier serverCapacitySupplier,
                      int leaseTtlMillis) {
        this.testDuration = testDuration;
        this.maxConcurrency = maxConcurrency;
        this.serverCapacitySupplier = serverCapacitySupplier;
        this.leaseTtlMillis = leaseTtlMillis;
        KeepAliveProvider keepAliveProvider = KeepAliveProvider.from(30_000, Flowable.interval(30, SECONDS));
        SetupProvider setup = SetupProvider.keepAlive(keepAliveProvider);
        setupProvider = serverCapacitySupplier == null? setup.disableLease() : setup;
    }

    public final Duration getTestDuration() {
        return testDuration;
    }

    public final int getMaxConcurrency() {
        return maxConcurrency;
    }

    public Flowable<Long> serverListChangeTicks() {
        return Flowable.interval(2, SECONDS);
    }

    public final ReactiveSocketClient newClientForServer(SocketAddress server) {
        TcpTransportClient transport = TcpTransportClient.create(server);
        ReactiveSocketClient raw = ReactiveSocketClient.create(transport, setupProvider);
        return wrap(detectFailures(connectTimeout(raw, 1, SECONDS, scheduler)),
                    timeout(1, SECONDS, scheduler));
    }

    public final LeaseEnforcingSocket nextServerHandler(int serverCount) {
        boolean bad = nextServerBad(serverCount);
        ReactiveSocket socket = bad? randomFailuresAndDelays() : alwaysPass();
        if (serverCapacitySupplier == null) {
            return new DisabledLeaseAcceptingSocket(socket);
        } else {
            FairLeaseDistributor leaseDistributor = new FairLeaseDistributor(serverCapacitySupplier, () -> leaseTtlMillis,
                                                                             Flowable.interval(0, leaseTtlMillis,
                                                                                               MILLISECONDS));
            return new DefaultLeaseEnforcingSocket(socket, leaseDistributor);
        }
    }

    protected boolean nextServerBad(int serverCount) {
        // 25% of bad servers
        return serverCount % 4 == 3;
    }
}
