/*
 * Copyright 2015-2018 the original author or authors.
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

package io.rsocket.transport.netty;

import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.Resume;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.test.PerfTest;
import io.rsocket.test.PingClient;
import io.rsocket.transport.netty.client.TcpClientTransport;
import java.time.Duration;
import org.HdrHistogram.Recorder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

@PerfTest
public final class TcpPing {
  private static final int INTERACTIONS_COUNT = 1_000_000_000;
  private static final int port = Integer.valueOf(System.getProperty("RSOCKET_TEST_PORT", "7878"));

  @BeforeEach
  void setUp() {
    System.out.println("Starting ping-pong test (TCP transport)");
    System.out.println("port: " + port);
  }

  @Test
  void requestResponseTest() {
    PingClient pingClient = newPingClient();
    Recorder recorder = pingClient.startTracker(Duration.ofSeconds(1));

    pingClient
        .requestResponsePingPong(INTERACTIONS_COUNT, recorder)
        .doOnTerminate(() -> System.out.println("Sent " + INTERACTIONS_COUNT + " messages."))
        .blockLast();
  }

  @Test
  void requestStreamTest() {
    PingClient pingClient = newPingClient();
    Recorder recorder = pingClient.startTracker(Duration.ofSeconds(1));

    pingClient
        .requestStreamPingPong(INTERACTIONS_COUNT, recorder)
        .doOnTerminate(() -> System.out.println("Sent " + INTERACTIONS_COUNT + " messages."))
        .blockLast();
  }

  @Test
  void requestStreamResumableTest() {
    PingClient pingClient = newResumablePingClient();
    Recorder recorder = pingClient.startTracker(Duration.ofSeconds(1));

    pingClient
        .requestStreamPingPong(INTERACTIONS_COUNT, recorder)
        .doOnTerminate(() -> System.out.println("Sent " + INTERACTIONS_COUNT + " messages."))
        .blockLast();
  }

  private static PingClient newPingClient() {
    return newPingClient(false);
  }

  private static PingClient newResumablePingClient() {
    return newPingClient(true);
  }

  private static PingClient newPingClient(boolean isResumable) {
    RSocketConnector connector = RSocketConnector.create();
    if (isResumable) {
      connector.resume(new Resume());
    }
    Mono<RSocket> rSocket =
        connector
            .payloadDecoder(PayloadDecoder.ZERO_COPY)
            .keepAlive(Duration.ofMinutes(1), Duration.ofMinutes(30))
            .connect(TcpClientTransport.create(port));

    return new PingClient(rSocket);
  }
}
