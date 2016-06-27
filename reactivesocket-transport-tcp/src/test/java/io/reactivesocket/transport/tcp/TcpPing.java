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
package io.reactivesocket.transport.tcp;

import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.test.PingClient;
import io.reactivesocket.transport.tcp.client.TcpReactiveSocketConnector;
import org.HdrHistogram.Recorder;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public final class TcpPing {

    public static void main(String... args) throws Exception {
        ConnectionSetupPayload payload = ConnectionSetupPayload.create("", "");
        TcpReactiveSocketConnector connector = TcpReactiveSocketConnector.create(payload, Throwable::printStackTrace);
        PingClient pingClient = new PingClient(connector);
        Recorder recorder = pingClient.startTracker(1, TimeUnit.SECONDS);
        final int count = 1_000_000;
        pingClient.connect(new InetSocketAddress("localhost", 7878))
                  .startPingPong(count, recorder)
                  .doOnTerminate(() -> {
                      System.out.println("Sent " + count + " messages.");
                  })
                  .toBlocking()
                  .last();
    }
}
