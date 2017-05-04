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

package io.rsocket.perf;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.perf.util.AbstractMicrobenchmarkBase;
import io.rsocket.perf.util.BlackholeSubscriber;
import io.rsocket.perf.util.ClientServerHolder;
import io.rsocket.transport.local.LocalClient;
import io.rsocket.transport.local.LocalServer;
import io.rsocket.transport.netty.client.TcpTransportClient;
import io.rsocket.transport.netty.server.TcpTransportServer;
import io.reactivex.Flowable;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.infra.Blackhole;
import reactor.ipc.netty.tcp.TcpClient;
import reactor.ipc.netty.tcp.TcpServer;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

public class AbstractRSocketPerf extends AbstractMicrobenchmarkBase {

    public static final String TRANSPORT_TCP_MULTI_CONNECTIONS = "tcp_multi_connections";
    public static final String TRANSPORT_TCP = "tcp";
    public static final String TRANSPORT_LOCAL = "local";

    @Param({ TRANSPORT_TCP_MULTI_CONNECTIONS, TRANSPORT_TCP, TRANSPORT_LOCAL })
    public String transport;

    protected Blackhole bh;
    protected Supplier<RSocket> localHolder;
    protected Supplier<RSocket> tcpHolder;
    protected Supplier<RSocket> multiClientTcpHolders;

    protected void _setup(Blackhole bh) {
        tcpHolder = ClientServerHolder.create(TcpTransportServer.create(TcpServer.create()), socketAddress ->
            TcpTransportClient.create(TcpClient.create(options -> options.connect((InetSocketAddress)socketAddress)))
        );
        String clientName = "local-" + ThreadLocalRandom.current().nextInt();
        localHolder = ClientServerHolder.create(LocalServer.create(clientName),
                                                         socketAddress -> LocalClient.create(clientName));
        multiClientTcpHolders = ClientServerHolder.requestResponseMultiTcp(Runtime.getRuntime().availableProcessors());
        this.bh = bh;
    }

    protected Supplier<RSocket> getSocketSupplier() {
        Supplier<RSocket> socketSupplier;
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
        return socketSupplier;
    }

    protected void requestResponse(Supplier<RSocket> socketSupplier, Supplier<Payload> payloadSupplier,
                                   int requestCount)
            throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(requestCount);
        for (int i = 0; i < requestCount; i++) {
            socketSupplier.get()
                          .requestResponse(payloadSupplier.get())
                          .subscribe(new BlackholeSubscriber<>(bh, () -> latch.countDown()));
        }
        latch.await();
    }

    protected void requestStream(Supplier<RSocket> socketSupplier, Supplier<Payload> payloadSupplier,
                                 int requestCount, int itemCount)
            throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(requestCount);
        for (int i = 0; i < requestCount; i++) {
            Flowable.fromPublisher(socketSupplier.get().requestStream(payloadSupplier.get()))
                    .take(itemCount)
                    .subscribe(new BlackholeSubscriber<>(bh, () -> latch.countDown()));
        }
        latch.await();
    }
}
