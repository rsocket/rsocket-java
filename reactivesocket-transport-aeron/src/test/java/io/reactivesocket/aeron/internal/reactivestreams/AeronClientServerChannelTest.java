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

package io.reactivesocket.aeron.internal.reactivestreams;

import io.reactivesocket.aeron.MediaDriverHolder;
import io.reactivesocket.aeron.internal.AeronWrapper;
import io.reactivesocket.aeron.internal.DefaultAeronWrapper;
import io.reactivesocket.aeron.internal.EventLoop;
import io.reactivesocket.aeron.internal.SingleThreadedEventLoop;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 */
@Ignore("travis does not like me")
public class AeronClientServerChannelTest {
    static {
        MediaDriverHolder.getInstance();
    }

    @Test(timeout = 5_000)
    public void testConnect() throws Exception {
        int clientId = ThreadLocalRandom.current().nextInt(0, 1_000);
        int serverId = clientId + 1;

        System.out.println("test client stream id => " + clientId);
        System.out.println("test server stream id => " + serverId);

        AeronWrapper aeronWrapper = new DefaultAeronWrapper();

        // Create Client Connector
        AeronSocketAddress clientManagementSocketAddress = AeronSocketAddress.create("aeron:udp", "127.0.0.1", 39790);
        EventLoop clientEventLoop = new SingleThreadedEventLoop("client");

        AeronSocketAddress receiveAddress = AeronSocketAddress.create("aeron:udp", "127.0.0.1", 39790);
        AeronSocketAddress sendAddress = AeronSocketAddress.create("aeron:udp", "127.0.0.1", 39790);

        AeronClientChannelConnector.AeronClientConfig config = AeronClientChannelConnector
            .AeronClientConfig.create(
                receiveAddress,
                sendAddress,
                clientId,
                serverId,
                clientEventLoop);

        AeronClientChannelConnector connector = AeronClientChannelConnector.create(aeronWrapper, clientManagementSocketAddress, clientEventLoop);

        // Create Server
        CountDownLatch latch = new CountDownLatch(2);

        AeronChannelServer.AeronChannelConsumer consumer = (AeronChannel aeronChannel) -> {
            Assert.assertNotNull(aeronChannel);
            latch.countDown();
        };

        AeronSocketAddress serverManagementSocketAddress = AeronSocketAddress.create("aeron:udp", "127.0.0.1", 39790);
        EventLoop serverEventLoop = new SingleThreadedEventLoop("server");
        AeronChannelServer aeronChannelServer = AeronChannelServer.create(consumer, aeronWrapper, serverManagementSocketAddress, serverEventLoop);

        aeronChannelServer
            .start();

        Publisher<AeronChannel> publisher = connector
            .apply(config);
        Flux
            .from(publisher)
            .doOnNext(Assert::assertNotNull)
            .doOnNext(c -> latch.countDown())
            .doOnError(t -> { throw new RuntimeException(t); })
            .subscribe();

        latch.await();

    }

    @Test(timeout = 5_000)
    public void testPingPong() throws Exception {
        int clientId = ThreadLocalRandom.current().nextInt(2_000, 3_000);
        int serverId = clientId + 1;

        System.out.println("test client stream id => " + clientId);
        System.out.println("test server stream id => " + serverId);

        AeronWrapper aeronWrapper = new DefaultAeronWrapper();

        // Create Client Connector
        AeronSocketAddress clientManagementSocketAddress = AeronSocketAddress.create("aeron:udp", "127.0.0.1", 39790);
        EventLoop clientEventLoop = new SingleThreadedEventLoop("client");

        AeronSocketAddress receiveAddress = AeronSocketAddress.create("aeron:udp", "127.0.0.1", 39790);
        AeronSocketAddress sendAddress = AeronSocketAddress.create("aeron:udp", "127.0.0.1", 39790);

        AeronClientChannelConnector.AeronClientConfig config = AeronClientChannelConnector
            .AeronClientConfig.create(
                receiveAddress,
                sendAddress,
                clientId,
                serverId,
                clientEventLoop);

        AeronClientChannelConnector connector = AeronClientChannelConnector.create(aeronWrapper, clientManagementSocketAddress, clientEventLoop);

        // Create Server

        AeronChannelServer.AeronChannelConsumer consumer = (AeronChannel aeronChannel) -> {
            Assert.assertNotNull(aeronChannel);

            Flux<? extends DirectBuffer> receive = aeronChannel
                .receive();

            Flux<? extends DirectBuffer> data = receive
                .doOnNext(b -> System.out.println("server received => " + b.getInt(0)));

            aeronChannel.send(data)
                .subscribe();
        };

        AeronSocketAddress serverManagementSocketAddress = AeronSocketAddress.create("aeron:udp", "127.0.0.1", 39790);
        EventLoop serverEventLoop = new SingleThreadedEventLoop("server");
        AeronChannelServer aeronChannelServer = AeronChannelServer.create(consumer, aeronWrapper, serverManagementSocketAddress, serverEventLoop);

        aeronChannelServer
            .start();

        Publisher<AeronChannel> publisher = connector
            .apply(config);

        int count = 10;
        CountDownLatch latch = new CountDownLatch(count);

        Mono.from(publisher)
              .then(aeronChannel ->
                               Mono.create(callback -> {
                                   Flux<UnsafeBuffer> data = Flux
                                           .range(1, count)
                                           .map(i -> {
                                               byte[] b = new byte[BitUtil.SIZE_OF_INT];
                                               UnsafeBuffer buffer = new UnsafeBuffer(b);
                                               buffer.putInt(0, i);
                                               return buffer;
                                           });

                                   aeronChannel.receive().doOnNext(b -> latch.countDown())
                                           .doOnNext(callback::success).subscribe();
                                   aeronChannel.send(data)
                                           .subscribe();
                               })
              )
              .subscribe();

        latch.await();

    }

}
