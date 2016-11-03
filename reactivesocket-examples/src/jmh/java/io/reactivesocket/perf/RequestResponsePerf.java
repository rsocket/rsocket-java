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

package io.reactivesocket.perf;

import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.local.LocalClient;
import io.reactivesocket.local.LocalServer;
import io.reactivesocket.perf.util.AbstractMicrobenchmarkBase;
import io.reactivesocket.perf.util.BlackholeSubscriber;
import io.reactivesocket.perf.util.ClientServerHolder;
import io.reactivesocket.transport.tcp.client.TcpTransportClient;
import io.reactivesocket.transport.tcp.server.TcpTransportServer;
import io.reactivesocket.util.PayloadImpl;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class RequestResponsePerf extends AbstractMicrobenchmarkBase {

    public static final String TRANSPORT_TCP_MULTI_CONNECTIONS = "tcp_multi_connections";
    public static final String TRANSPORT_TCP = "tcp";
    public static final String TRANSPORT_LOCAL = "local";

    @Param({ TRANSPORT_TCP_MULTI_CONNECTIONS, TRANSPORT_TCP, TRANSPORT_LOCAL })
    public String transport;

    @Param({ "1", "100", "1000"})
    public int requestCount;

    public Blackhole bh;

    public Supplier<ReactiveSocket> localHolder;
    public Supplier<ReactiveSocket> tcpHolder;
    public Supplier<ReactiveSocket> multiClientTcpHolders;

    @Setup(Level.Trial)
    public void setup(Blackhole bh) {
        tcpHolder = ClientServerHolder.requestResponse(TcpTransportServer.create(),
                                                       socketAddress -> TcpTransportClient.create(socketAddress));
        String clientName = "local-" + ThreadLocalRandom.current().nextInt();
        localHolder = ClientServerHolder.requestResponse(LocalServer.create(clientName),
                                                         socketAddress -> LocalClient.create(clientName));
        multiClientTcpHolders = ClientServerHolder.requestResponseMultiTcp(Runtime.getRuntime().availableProcessors());
        this.bh = bh;
    }

    @Benchmark
    public void requestResponse() throws InterruptedException {
        Supplier<ReactiveSocket> socketSupplier;
        switch (transport) {
        case TRANSPORT_LOCAL:
            socketSupplier = localHolder;
            break;
        case TRANSPORT_TCP:
            socketSupplier = tcpHolder;
            break;
        case TRANSPORT_TCP_MULTI_CONNECTIONS:
            socketSupplier = multiClientTcpHolders;
            break;
        default:
            throw new IllegalArgumentException("Unknown transport: " + transport);
        }
        requestResponse(socketSupplier);
    }

    protected void requestResponse(Supplier<ReactiveSocket> socketSupplier) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(requestCount);
        for (int i = 0; i < requestCount; i++) {
            socketSupplier.get()
                          .requestResponse(new PayloadImpl(ClientServerHolder.HELLO))
                          .subscribe(new BlackholeSubscriber<>(bh, () -> latch.countDown()));
        }
        latch.await();
    }
}
