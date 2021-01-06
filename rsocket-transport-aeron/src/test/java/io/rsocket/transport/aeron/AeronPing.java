/*
 * Copyright 2015-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.transport.aeron;

import static java.util.concurrent.TimeUnit.SECONDS;

import io.aeron.Aeron;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.test.PingClient;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.time.Duration;
import org.HdrHistogram.Recorder;
import org.agrona.concurrent.BackoffIdleStrategy;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public final class AeronPing {

  public static void main(String... args) {
    Aeron aeron = Aeron.connect();
    ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

    String aeronUrl = "aeron:ipc";

    AeronClientTransport aeronClientTransport =
        new AeronClientTransport(
            aeron,
            aeronUrl,
            Schedulers.newSingle("AeronScheduler"),
            new EventLoopGroup(2, () -> new BackoffIdleStrategy(100, 1000, 10000, 1000000)),
            new BackoffIdleStrategy(100, 1000, 10000, 1000000),
            allocator,
            256,
            32,
            SECONDS.toNanos(5));

    //    TcpClientTransport tcpClientTransport = TcpClientTransport.create(8080);
    //
    //    WebsocketClientTransport websocketClientTransport = WebsocketClientTransport.create(8080);

    Mono<RSocket> client =
        RSocketConnector.create()
            .payloadDecoder(PayloadDecoder.ZERO_COPY)
            .connect(aeronClientTransport);

    RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();

    // Get name representing the running Java virtual machine.
    // It returns something like 6460@AURORA. Where the value
    // before the @ symbol is the PID.
    String jvmName = bean.getName();
    System.out.println("Name = " + jvmName);

    // Extract the PID by splitting the string returned by the
    // bean.getName() method.
    long pid = Long.valueOf(jvmName.split("@")[0]);

    System.out.println("Client connected to channel[" + aeronUrl + "]. PID  = " + pid);

    PingClient pingClient = new PingClient(client);
    Recorder recorder = pingClient.startTracker(Duration.ofSeconds(1));
    final int count = 1_000_000_000;
    pingClient
        .requestResponsePingPong(count, recorder)
        .doOnTerminate(() -> System.out.println("Sent " + count + " messages."))
        .blockLast();

    //    RSocket rSocket = client.block();
    //
    //    for (int i = 0; i < 10000; i++) {
    //      rSocket.requestResponse(DefaultPayload.create("Message : " + i)).block();
    //    }

    System.exit(0);
  }
}
