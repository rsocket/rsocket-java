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

package io.rsocket.core;

import static org.junit.Assert.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
import io.rsocket.frame.ErrorFrameCodec;
import io.rsocket.frame.KeepAliveFrameCodec;
import io.rsocket.frame.LeaseFrameCodec;
import io.rsocket.frame.MetadataPushFrameCodec;
import io.rsocket.plugins.InitializingInterceptorRegistry;
import io.rsocket.test.util.TestDuplexConnection;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ClientServerInputMultiplexerTest {
  private TestDuplexConnection source;
  private ClientServerInputMultiplexer clientMultiplexer;
  private LeaksTrackingByteBufAllocator allocator =
      LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
  private ClientServerInputMultiplexer serverMultiplexer;

  @BeforeEach
  public void setup() {
    source = new TestDuplexConnection(allocator);
    clientMultiplexer =
        new ClientServerInputMultiplexer(source, new InitializingInterceptorRegistry(), true);
    serverMultiplexer =
        new ClientServerInputMultiplexer(source, new InitializingInterceptorRegistry(), false);
  }

  @Test
  public void clientSplits() {
    AtomicInteger clientFrames = new AtomicInteger();
    AtomicInteger serverFrames = new AtomicInteger();

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

    source.addToReceivedBuffer(errorFrame(1).retain());
    assertEquals(1, clientFrames.get());
    assertEquals(0, serverFrames.get());

    source.addToReceivedBuffer(errorFrame(1).retain());
    assertEquals(2, clientFrames.get());
    assertEquals(0, serverFrames.get());

    source.addToReceivedBuffer(leaseFrame().retain());
    assertEquals(3, clientFrames.get());
    assertEquals(0, serverFrames.get());

    source.addToReceivedBuffer(keepAliveFrame().retain());
    assertEquals(4, clientFrames.get());
    assertEquals(0, serverFrames.get());

    source.addToReceivedBuffer(errorFrame(2).retain());
    assertEquals(4, clientFrames.get());
    assertEquals(1, serverFrames.get());

    source.addToReceivedBuffer(errorFrame(0).retain());
    assertEquals(5, clientFrames.get());
    assertEquals(1, serverFrames.get());

    source.addToReceivedBuffer(metadataPushFrame().retain());
    assertEquals(5, clientFrames.get());
    assertEquals(2, serverFrames.get());
  }

  @Test
  public void serverSplits() {
    AtomicInteger clientFrames = new AtomicInteger();
    AtomicInteger serverFrames = new AtomicInteger();

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

    source.addToReceivedBuffer(errorFrame(1).retain());
    assertEquals(1, clientFrames.get());
    assertEquals(0, serverFrames.get());

    source.addToReceivedBuffer(errorFrame(1).retain());
    assertEquals(2, clientFrames.get());
    assertEquals(0, serverFrames.get());

    source.addToReceivedBuffer(leaseFrame().retain());
    assertEquals(2, clientFrames.get());
    assertEquals(1, serverFrames.get());

    source.addToReceivedBuffer(keepAliveFrame().retain());
    assertEquals(2, clientFrames.get());
    assertEquals(2, serverFrames.get());

    source.addToReceivedBuffer(errorFrame(2).retain());
    assertEquals(2, clientFrames.get());
    assertEquals(3, serverFrames.get());

    source.addToReceivedBuffer(errorFrame(0).retain());
    assertEquals(2, clientFrames.get());
    assertEquals(4, serverFrames.get());

    source.addToReceivedBuffer(metadataPushFrame().retain());
    assertEquals(3, clientFrames.get());
    assertEquals(4, serverFrames.get());
  }

  private ByteBuf leaseFrame() {
    return LeaseFrameCodec.encode(allocator, 1_000, 1, Unpooled.EMPTY_BUFFER);
  }

  private ByteBuf errorFrame(int i) {
    return ErrorFrameCodec.encode(allocator, i, new Exception());
  }

  private ByteBuf keepAliveFrame() {
    return KeepAliveFrameCodec.encode(allocator, false, 0, Unpooled.EMPTY_BUFFER);
  }

  private ByteBuf metadataPushFrame() {
    return MetadataPushFrameCodec.encode(allocator, Unpooled.EMPTY_BUFFER);
  }
}
