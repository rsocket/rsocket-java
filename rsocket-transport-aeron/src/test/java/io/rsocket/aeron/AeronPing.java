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
package io.rsocket.aeron;

import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.aeron.client.AeronClientTransport;
import io.rsocket.aeron.internal.*;
import io.rsocket.aeron.internal.reactivestreams.AeronClientChannelConnector;
import io.rsocket.aeron.internal.reactivestreams.AeronSocketAddress;
import io.rsocket.test.PingClient;
import java.time.Duration;
import org.HdrHistogram.Recorder;
import reactor.core.publisher.Mono;

public final class AeronPing {

  public static void main(String... args) {
    // Create Client Connector
    AeronWrapper aeronWrapper = new DefaultAeronWrapper();

    AeronSocketAddress clientManagementSocketAddress =
        AeronSocketAddress.create("aeron:udp", "127.0.0.1", 39790);
    EventLoop clientEventLoop = new SingleThreadedEventLoop("client");

    AeronSocketAddress receiveAddress = AeronSocketAddress.create("aeron:udp", "127.0.0.1", 39790);
    AeronSocketAddress sendAddress = AeronSocketAddress.create("aeron:udp", "127.0.0.1", 39790);

    AeronClientChannelConnector.AeronClientConfig config =
        AeronClientChannelConnector.AeronClientConfig.create(
            receiveAddress,
            sendAddress,
            Constants.CLIENT_STREAM_ID,
            Constants.SERVER_STREAM_ID,
            clientEventLoop);

    AeronClientChannelConnector connector =
        AeronClientChannelConnector.create(
            aeronWrapper, clientManagementSocketAddress, clientEventLoop);

    AeronClientTransport aeronTransportClient = new AeronClientTransport(connector, config);

    Mono<RSocket> client = RSocketFactory.connect().transport(aeronTransportClient).start();
    PingClient pingClient = new PingClient(client);
    Recorder recorder = pingClient.startTracker(Duration.ofSeconds(1));
    final int count = 1_000_000_000;
    pingClient
        .startPingPong(count, recorder)
        .doOnTerminate(() -> System.out.println("Sent " + count + " messages."))
        .blockLast();

    System.exit(0);
  }
}
