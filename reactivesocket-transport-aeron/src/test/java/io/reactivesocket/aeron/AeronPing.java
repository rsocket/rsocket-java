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
package io.reactivesocket.aeron;

import io.reactivesocket.aeron.client.AeronTransportClient;
import io.reactivesocket.aeron.internal.AeronWrapper;
import io.reactivesocket.aeron.internal.Constants;
import io.reactivesocket.aeron.internal.DefaultAeronWrapper;
import io.reactivesocket.aeron.internal.EventLoop;
import io.reactivesocket.aeron.internal.SingleThreadedEventLoop;
import io.reactivesocket.aeron.internal.reactivestreams.AeronClientChannelConnector;
import io.reactivesocket.aeron.internal.reactivestreams.AeronSocketAddress;
import io.reactivesocket.client.KeepAliveProvider;
import io.reactivesocket.client.ReactiveSocketClient;
import io.reactivesocket.client.SetupProvider;
import io.reactivesocket.test.PingClient;
import org.HdrHistogram.Recorder;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public final class AeronPing {

    public static void main(String... args) throws Exception {
        SetupProvider setup = SetupProvider.keepAlive(KeepAliveProvider.never()).disableLease();

        // Create Client Connector
        AeronWrapper aeronWrapper = new DefaultAeronWrapper();

        AeronSocketAddress clientManagementSocketAddress = AeronSocketAddress.create("aeron:udp", "127.0.0.1", 39790);
        EventLoop clientEventLoop = new SingleThreadedEventLoop("client");

        AeronSocketAddress receiveAddress = AeronSocketAddress.create("aeron:udp", "127.0.0.1", 39790);
        AeronSocketAddress sendAddress = AeronSocketAddress.create("aeron:udp", "127.0.0.1", 39790);

        AeronClientChannelConnector.AeronClientConfig config = AeronClientChannelConnector
            .AeronClientConfig.create(
                receiveAddress,
                sendAddress,
                Constants.CLIENT_STREAM_ID,
                Constants.SERVER_STREAM_ID,
                clientEventLoop);

        AeronClientChannelConnector connector = AeronClientChannelConnector
            .create(aeronWrapper,
                clientManagementSocketAddress,
                clientEventLoop);

        AeronTransportClient aeronTransportClient = new AeronTransportClient(connector, config);

        ReactiveSocketClient client =
                ReactiveSocketClient.create(aeronTransportClient, setup);
        PingClient pingClient = new PingClient(client);
        Recorder recorder = pingClient.startTracker(1, TimeUnit.SECONDS);
        final int count = 1_000_000_000;
        pingClient.connect()
                  .startPingPong(count, recorder)
                  .doOnTerminate(() -> {
                      System.out.println("Sent " + count + " messages.");
                  })
                  .last(null).blockingGet();

        System.exit(0);
    }
}
