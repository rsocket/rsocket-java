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
import io.rsocket.client.KeepAliveProvider;
import io.rsocket.client.RSocketClient;
import io.rsocket.client.SetupProvider;
import io.rsocket.lease.DefaultLeaseEnforcingSocket;
import io.rsocket.lease.DisabledLeaseAcceptingSocket;
import io.rsocket.lease.FairLeaseDistributor;
import io.rsocket.lease.LeaseEnforcingSocket;
import io.rsocket.transport.netty.client.TcpTransportClient;
import reactor.core.publisher.Flux;
import reactor.ipc.netty.tcp.TcpClient;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.function.IntSupplier;

import static io.rsocket.client.filter.RSocketClients.*;
import static io.rsocket.client.filter.RSockets.*;
import static io.rsocket.examples.transport.tcp.stress.StressTestHandler.*;

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
        KeepAliveProvider keepAliveProvider = KeepAliveProvider.from(30_000, Flux.interval(Duration.ofSeconds(30)));
        SetupProvider setup = SetupProvider.keepAlive(keepAliveProvider);
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

    public final RSocketClient newClientForServer(SocketAddress server) {
        TcpTransportClient transport = TcpTransportClient.create(TcpClient.create(options ->
                options.connect((InetSocketAddress)server)));
        RSocketClient raw = RSocketClient.create(transport, setupProvider);
        return wrap(detectFailures(connectTimeout(raw, Duration.ofSeconds(1))),
                    timeout(Duration.ofSeconds(1)));
    }

    public final LeaseEnforcingSocket nextServerHandler(int serverCount) {
        boolean bad = nextServerBad(serverCount);
        RSocket socket = bad? randomFailuresAndDelays() : alwaysPass();
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
