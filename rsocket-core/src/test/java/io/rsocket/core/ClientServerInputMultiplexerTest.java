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

import static org.assertj.core.api.Assertions.assertThat;

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
        .doOnNext(
            f -> {
              clientFrames.incrementAndGet();
              f.release();
            })
        .subscribe();
    clientMultiplexer
        .asServerConnection()
        .receive()
        .doOnNext(
            f -> {
              serverFrames.incrementAndGet();
              f.release();
            })
        .subscribe();

    source.addToReceivedBuffer(errorFrame(1).retain());
    assertThat(clientFrames.get()).isOne();
    assertThat(serverFrames.get()).isZero();

    source.addToReceivedBuffer(errorFrame(1).retain());
    assertThat(clientFrames.get()).isEqualTo(2);
    assertThat(serverFrames.get()).isZero();

    source.addToReceivedBuffer(leaseFrame().retain());
    assertThat(clientFrames.get()).isEqualTo(3);
    assertThat(serverFrames.get()).isZero();

    source.addToReceivedBuffer(keepAliveFrame().retain());
    assertThat(clientFrames.get()).isEqualTo(4);
    assertThat(serverFrames.get()).isZero();

    source.addToReceivedBuffer(errorFrame(2).retain());
    assertThat(clientFrames.get()).isEqualTo(4);
    assertThat(serverFrames.get()).isOne();

    source.addToReceivedBuffer(errorFrame(0).retain());
    assertThat(clientFrames.get()).isEqualTo(5);
    assertThat(serverFrames.get()).isOne();

    source.addToReceivedBuffer(metadataPushFrame().retain());
    assertThat(clientFrames.get()).isEqualTo(5);
    assertThat(serverFrames.get()).isEqualTo(2);
  }

  @Test
  public void serverSplits() {
    AtomicInteger clientFrames = new AtomicInteger();
    AtomicInteger serverFrames = new AtomicInteger();

    serverMultiplexer
        .asClientConnection()
        .receive()
        .doOnNext(
            f -> {
              clientFrames.incrementAndGet();
              f.release();
            })
        .subscribe();
    serverMultiplexer
        .asServerConnection()
        .receive()
        .doOnNext(
            f -> {
              serverFrames.incrementAndGet();
              f.release();
            })
        .subscribe();

    source.addToReceivedBuffer(errorFrame(1).retain());
    assertThat(clientFrames.get()).isEqualTo(1);
    assertThat(serverFrames.get()).isZero();

    source.addToReceivedBuffer(errorFrame(1).retain());
    assertThat(clientFrames.get()).isEqualTo(2);
    assertThat(serverFrames.get()).isZero();

    source.addToReceivedBuffer(leaseFrame().retain());
    assertThat(clientFrames.get()).isEqualTo(2);
    assertThat(serverFrames.get()).isOne();

    source.addToReceivedBuffer(keepAliveFrame().retain());
    assertThat(clientFrames.get()).isEqualTo(2);
    assertThat(serverFrames.get()).isEqualTo(2);

    source.addToReceivedBuffer(errorFrame(2).retain());
    assertThat(clientFrames.get()).isEqualTo(2);
    assertThat(serverFrames.get()).isEqualTo(3);

    source.addToReceivedBuffer(errorFrame(0).retain());
    assertThat(clientFrames.get()).isEqualTo(2);
    assertThat(serverFrames.get()).isEqualTo(4);

    source.addToReceivedBuffer(metadataPushFrame().retain());
    assertThat(clientFrames.get()).isEqualTo(3);
    assertThat(serverFrames.get()).isEqualTo(4);
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
