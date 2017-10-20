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
package io.rsocket.transport.netty;

import io.rsocket.Frame;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.test.PingClient;
import io.rsocket.transport.netty.client.TcpClientTransport;
import java.time.Duration;

import org.HdrHistogram.Recorder;
import reactor.core.publisher.Mono;

public final class TcpPing {

  public static void main(String... args) {
    Mono<RSocket> client =
        RSocketFactory.connect()
            .frameDecoder(Frame::retain)
            .transport(TcpClientTransport.create(7878))
            .start();

    PingClient pingClient = new PingClient(client);
    Recorder recorder = pingClient.startTracker(Duration.ofSeconds(1));
    final int count = 1_000_000_000;
    pingClient
        .startPingPong(count, recorder)
        .doOnTerminate(() -> System.out.println("Sent " + count + " messages."))
        .blockLast();
  }
}
