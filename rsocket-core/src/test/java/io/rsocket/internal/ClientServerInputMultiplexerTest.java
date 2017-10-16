/*
 * Copyright 2016 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.rsocket.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.rsocket.Frame;
import io.rsocket.plugins.PluginRegistry;
import io.rsocket.test.util.TestDuplexConnection;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ClientServerInputMultiplexerTest {
  private TestDuplexConnection source;
  private ClientServerInputMultiplexer multiplexer;

  @BeforeEach
  public void setup() {
    source = new TestDuplexConnection();
    multiplexer = new ClientServerInputMultiplexer(source, new PluginRegistry());
  }

  @Test
  public void testSplits() {
    AtomicInteger clientFrames = new AtomicInteger();
    AtomicInteger serverFrames = new AtomicInteger();
    AtomicInteger connectionFrames = new AtomicInteger();

    multiplexer
        .asClientConnection()
        .receive()
        .doOnNext(f -> clientFrames.incrementAndGet())
        .subscribe();
    multiplexer
        .asServerConnection()
        .receive()
        .doOnNext(f -> serverFrames.incrementAndGet())
        .subscribe();
    multiplexer
        .asStreamZeroConnection()
        .receive()
        .doOnNext(f -> connectionFrames.incrementAndGet())
        .subscribe();

    source.addToReceivedBuffer(Frame.Error.from(1, new Exception()));
    assertEquals(1, clientFrames.get());
    assertEquals(0, serverFrames.get());
    assertEquals(0, connectionFrames.get());

    source.addToReceivedBuffer(Frame.Error.from(2, new Exception()));
    assertEquals(1, clientFrames.get());
    assertEquals(1, serverFrames.get());
    assertEquals(0, connectionFrames.get());

    source.addToReceivedBuffer(Frame.Error.from(1, new Exception()));
    assertEquals(2, clientFrames.get());
    assertEquals(1, serverFrames.get());
    assertEquals(0, connectionFrames.get());
  }
}
