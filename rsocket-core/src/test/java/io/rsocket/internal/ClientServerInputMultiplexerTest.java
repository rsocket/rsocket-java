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

package io.rsocket.internal;

import static org.junit.Assert.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.frame.*;
import io.rsocket.plugins.PluginRegistry;
import io.rsocket.test.util.TestDuplexConnection;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;

public class ClientServerInputMultiplexerTest {
  private TestDuplexConnection source;
  private ClientServerInputMultiplexer clientMultiplexer;
  private ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
  private ClientServerInputMultiplexer serverMultiplexer;

  @Before
  public void setup() {
    source = new TestDuplexConnection();
    clientMultiplexer = new ClientServerInputMultiplexer(source, new PluginRegistry(), true);
    serverMultiplexer = new ClientServerInputMultiplexer(source, new PluginRegistry(), false);
  }

  @Test
  public void clientSplits() {
    AtomicInteger clientFrames = new AtomicInteger();
    AtomicInteger serverFrames = new AtomicInteger();
    AtomicInteger setupFrames = new AtomicInteger();

    clientMultiplexer
        .asClientConnection()
        .receive()
        .doOnNext(f -> clientFrames.incrementAndGet())
        .subscribe();
    clientMultiplexer
        .asServerConnection()
        .receive()
        .doOnNext(f -> serverFrames.incrementAndGet())
        .subscribe();
    clientMultiplexer
        .asSetupConnection()
        .receive()
        .doOnNext(f -> setupFrames.incrementAndGet())
        .subscribe();

    source.addToReceivedBuffer(errorFrame(1));
    assertEquals(1, clientFrames.get());
    assertEquals(0, serverFrames.get());
    assertEquals(0, setupFrames.get());

    source.addToReceivedBuffer(errorFrame(1));
    assertEquals(2, clientFrames.get());
    assertEquals(0, serverFrames.get());
    assertEquals(0, setupFrames.get());

    source.addToReceivedBuffer(leaseFrame());
    assertEquals(3, clientFrames.get());
    assertEquals(0, serverFrames.get());
    assertEquals(0, setupFrames.get());

    source.addToReceivedBuffer(keepAliveFrame());
    assertEquals(4, clientFrames.get());
    assertEquals(0, serverFrames.get());
    assertEquals(0, setupFrames.get());

    source.addToReceivedBuffer(errorFrame(2));
    assertEquals(4, clientFrames.get());
    assertEquals(1, serverFrames.get());
    assertEquals(0, setupFrames.get());

    source.addToReceivedBuffer(errorFrame(0));
    assertEquals(5, clientFrames.get());
    assertEquals(1, serverFrames.get());
    assertEquals(0, setupFrames.get());

    source.addToReceivedBuffer(metadataPushFrame());
    assertEquals(5, clientFrames.get());
    assertEquals(2, serverFrames.get());
    assertEquals(0, setupFrames.get());

    source.addToReceivedBuffer(setupFrame());
    assertEquals(5, clientFrames.get());
    assertEquals(2, serverFrames.get());
    assertEquals(1, setupFrames.get());

    source.addToReceivedBuffer(resumeFrame());
    assertEquals(5, clientFrames.get());
    assertEquals(2, serverFrames.get());
    assertEquals(2, setupFrames.get());

    source.addToReceivedBuffer(resumeOkFrame());
    assertEquals(5, clientFrames.get());
    assertEquals(2, serverFrames.get());
    assertEquals(3, setupFrames.get());
  }

  @Test
  public void serverSplits() {
    AtomicInteger clientFrames = new AtomicInteger();
    AtomicInteger serverFrames = new AtomicInteger();
    AtomicInteger setupFrames = new AtomicInteger();

    serverMultiplexer
        .asClientConnection()
        .receive()
        .doOnNext(f -> clientFrames.incrementAndGet())
        .subscribe();
    serverMultiplexer
        .asServerConnection()
        .receive()
        .doOnNext(f -> serverFrames.incrementAndGet())
        .subscribe();
    serverMultiplexer
        .asSetupConnection()
        .receive()
        .doOnNext(f -> setupFrames.incrementAndGet())
        .subscribe();

    source.addToReceivedBuffer(errorFrame(1));
    assertEquals(1, clientFrames.get());
    assertEquals(0, serverFrames.get());
    assertEquals(0, setupFrames.get());

    source.addToReceivedBuffer(errorFrame(1));
    assertEquals(2, clientFrames.get());
    assertEquals(0, serverFrames.get());
    assertEquals(0, setupFrames.get());

    source.addToReceivedBuffer(leaseFrame());
    assertEquals(2, clientFrames.get());
    assertEquals(1, serverFrames.get());
    assertEquals(0, setupFrames.get());

    source.addToReceivedBuffer(keepAliveFrame());
    assertEquals(2, clientFrames.get());
    assertEquals(2, serverFrames.get());
    assertEquals(0, setupFrames.get());

    source.addToReceivedBuffer(errorFrame(2));
    assertEquals(2, clientFrames.get());
    assertEquals(3, serverFrames.get());
    assertEquals(0, setupFrames.get());

    source.addToReceivedBuffer(errorFrame(0));
    assertEquals(2, clientFrames.get());
    assertEquals(4, serverFrames.get());
    assertEquals(0, setupFrames.get());

    source.addToReceivedBuffer(metadataPushFrame());
    assertEquals(3, clientFrames.get());
    assertEquals(4, serverFrames.get());
    assertEquals(0, setupFrames.get());

    source.addToReceivedBuffer(setupFrame());
    assertEquals(3, clientFrames.get());
    assertEquals(4, serverFrames.get());
    assertEquals(1, setupFrames.get());

    source.addToReceivedBuffer(resumeFrame());
    assertEquals(3, clientFrames.get());
    assertEquals(4, serverFrames.get());
    assertEquals(2, setupFrames.get());

    source.addToReceivedBuffer(resumeOkFrame());
    assertEquals(3, clientFrames.get());
    assertEquals(4, serverFrames.get());
    assertEquals(3, setupFrames.get());
  }

  private ByteBuf resumeFrame() {
    return ResumeFrameFlyweight.encode(allocator, Unpooled.EMPTY_BUFFER, 0, 0);
  }

  private ByteBuf setupFrame() {
    return SetupFrameFlyweight.encode(
        ByteBufAllocator.DEFAULT,
        false,
        0,
        42,
        "application/octet-stream",
        "application/octet-stream",
        Unpooled.EMPTY_BUFFER,
        Unpooled.EMPTY_BUFFER);
  }

  private ByteBuf leaseFrame() {
    return LeaseFrameFlyweight.encode(allocator, 1_000, 1, Unpooled.EMPTY_BUFFER);
  }

  private ByteBuf errorFrame(int i) {
    return ErrorFrameFlyweight.encode(allocator, i, new Exception());
  }

  private ByteBuf resumeOkFrame() {
    return ResumeOkFrameFlyweight.encode(allocator, 0);
  }

  private ByteBuf keepAliveFrame() {
    return KeepAliveFrameFlyweight.encode(allocator, false, 0, Unpooled.EMPTY_BUFFER);
  }

  private ByteBuf metadataPushFrame() {
    return MetadataPushFrameFlyweight.encode(allocator, Unpooled.EMPTY_BUFFER);
  }
}
