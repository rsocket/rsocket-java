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
package io.rsocket.transport.local;

import io.rsocket.client.KeepAliveProvider;
import io.rsocket.client.RSocketClient;
import io.rsocket.client.SetupProvider;
import io.rsocket.server.RSocketServer;
import io.rsocket.test.PingClient;
import io.rsocket.test.PingHandler;
import org.HdrHistogram.Recorder;

import java.time.Duration;

public final class LocalPingPong {

    public static void main(String... args) throws Exception {
        RSocketServer.create(LocalServer.create("test-local-server"))
                .start(new PingHandler());

        SetupProvider setup = SetupProvider.keepAlive(KeepAliveProvider.never()).disableLease();
        RSocketClient client =
                RSocketClient.create(LocalClient.create("test-local-server"), setup);
        PingClient pingClient = new PingClient(client);
        Recorder recorder = pingClient.startTracker(Duration.ofSeconds(1));
        final int count = 1_000_000_000;
        pingClient.connect()
                  .startPingPong(count, recorder)
                  .doOnTerminate(() -> {
                      System.out.println("Sent " + count + " messages.");
                  })
                  .blockLast();
    }
}
