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
import io.reactivesocket.transport.netty.client.TcpTransportClient;
import reactor.core.publisher.Flux;
import reactor.ipc.netty.tcp.TcpClient;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.function.IntSupplier;

import static io.reactivesocket.client.filter.ReactiveSocketClients.*;
import static io.reactivesocket.client.filter.ReactiveSockets.*;
import static io.reactivesocket.examples.transport.tcp.stress.StressTestHandler.*;

public class TestConfig {

    private final Duration testDuration;
    private final int maxConcurrency;
    private final IntSupplier serverCapacitySupplier;
    private final int leaseTtlMillis;
    private final SetupProvider setupProvider;

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
        SetupProvider setup = SetupProvider.keepAlive(KeepAliveProvider.from(30_000));
        setupProvider = serverCapacitySupplier == null? setup.disableLease() : setup;
    }

    public final Duration getTestDuration() {
        return testDuration;
    }

    public final int getMaxConcurrency() {
        return maxConcurrency;
    }

    public Flux<Long> serverListChangeTicks() {
        return Flux.interval(Duration.ofSeconds(2));
    }

    public final ReactiveSocketClient newClientForServer(SocketAddress server) {
        TcpTransportClient transport = TcpTransportClient.create(TcpClient.create(options ->
                options.connect((InetSocketAddress)server)));
        ReactiveSocketClient raw = ReactiveSocketClient.create(transport, setupProvider);
        return wrap(detectFailures(connectTimeout(raw, Duration.ofSeconds(1))),
                    timeout(Duration.ofSeconds(1)));
    }

    public final LeaseEnforcingSocket nextServerHandler(int serverCount) {
        boolean bad = nextServerBad(serverCount);
        ReactiveSocket socket = bad? randomFailuresAndDelays() : alwaysPass();
        if (serverCapacitySupplier == null) {
            return new DisabledLeaseAcceptingSocket(socket);
        } else {
            FairLeaseDistributor leaseDistributor = new FairLeaseDistributor(serverCapacitySupplier, leaseTtlMillis,
                                                                             Flux.interval(Duration.ofMillis(leaseTtlMillis)));
            return new DefaultLeaseEnforcingSocket(socket, leaseDistributor);
        }
    }

    protected boolean nextServerBad(int serverCount) {
        // 25% of bad servers
        return serverCount % 4 == 3;
    }
}
