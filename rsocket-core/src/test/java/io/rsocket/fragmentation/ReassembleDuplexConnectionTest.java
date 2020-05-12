/*
 * Copyright 2015-2020 the original author or authors.
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

package io.rsocket.fragmentation;

import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.DuplexConnection;
import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
import io.rsocket.frame.CancelFrameCodec;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.PayloadFrameCodec;
import io.rsocket.frame.RequestResponseFrameCodec;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Assert;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

final class ReassembleDuplexConnectionTest {
  private static byte[] data = new byte[1024];
  private static byte[] metadata = new byte[1024];

  static {
    ThreadLocalRandom.current().nextBytes(data);
    ThreadLocalRandom.current().nextBytes(metadata);
  }

  private final DuplexConnection delegate = mock(DuplexConnection.class, RETURNS_SMART_NULLS);

  private LeaksTrackingByteBufAllocator allocator =
      LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);

  @DisplayName("reassembles data")
  @Test
  void reassembleData() {
    List<ByteBuf> byteBufs =
        Arrays.asList(
            RequestResponseFrameCodec.encode(
                allocator, 1, true, null, Unpooled.wrappedBuffer(data)),
            PayloadFrameCodec.encode(
                allocator, 1, true, false, true, null, Unpooled.wrappedBuffer(data)),
            PayloadFrameCodec.encode(
                allocator, 1, true, false, true, null, Unpooled.wrappedBuffer(data)),
            PayloadFrameCodec.encode(
                allocator, 1, true, false, true, null, Unpooled.wrappedBuffer(data)),
            PayloadFrameCodec.encode(
                allocator, 1, false, false, true, null, Unpooled.wrappedBuffer(data)));

    CompositeByteBuf data =
        allocator
            .compositeDirectBuffer()
            .addComponents(
                true,
                Unpooled.wrappedBuffer(ReassembleDuplexConnectionTest.data),
                Unpooled.wrappedBuffer(ReassembleDuplexConnectionTest.data),
                Unpooled.wrappedBuffer(ReassembleDuplexConnectionTest.data),
                Unpooled.wrappedBuffer(ReassembleDuplexConnectionTest.data),
                Unpooled.wrappedBuffer(ReassembleDuplexConnectionTest.data));

    when(delegate.receive()).thenReturn(Flux.fromIterable(byteBufs));
    when(delegate.onClose()).thenReturn(Mono.never());
    when(delegate.alloc()).thenReturn(allocator);

    new ReassemblyDuplexConnection(delegate, false)
        .receive()
        .as(StepVerifier::create)
        .assertNext(
            byteBuf -> {
              Assert.assertEquals(data, RequestResponseFrameCodec.data(byteBuf));
            })
        .verifyComplete();
  }

  @DisplayName("reassembles metadata")
  @Test
  void reassembleMetadata() {
    List<ByteBuf> byteBufs =
        Arrays.asList(
            RequestResponseFrameCodec.encode(
                allocator, 1, true, Unpooled.wrappedBuffer(metadata), Unpooled.EMPTY_BUFFER),
            PayloadFrameCodec.encode(
                allocator,
                1,
                true,
                false,
                true,
                Unpooled.wrappedBuffer(metadata),
                Unpooled.EMPTY_BUFFER),
            PayloadFrameCodec.encode(
                allocator,
                1,
                true,
                false,
                true,
                Unpooled.wrappedBuffer(metadata),
                Unpooled.EMPTY_BUFFER),
            PayloadFrameCodec.encode(
                allocator,
                1,
                true,
                false,
                true,
                Unpooled.wrappedBuffer(metadata),
                Unpooled.EMPTY_BUFFER),
            PayloadFrameCodec.encode(
                allocator,
                1,
                false,
                false,
                true,
                Unpooled.wrappedBuffer(metadata),
                Unpooled.EMPTY_BUFFER));

    CompositeByteBuf metadata =
        allocator
            .compositeDirectBuffer()
            .addComponents(
                true,
                Unpooled.wrappedBuffer(ReassembleDuplexConnectionTest.metadata),
                Unpooled.wrappedBuffer(ReassembleDuplexConnectionTest.metadata),
                Unpooled.wrappedBuffer(ReassembleDuplexConnectionTest.metadata),
                Unpooled.wrappedBuffer(ReassembleDuplexConnectionTest.metadata),
                Unpooled.wrappedBuffer(ReassembleDuplexConnectionTest.metadata));

    when(delegate.receive()).thenReturn(Flux.fromIterable(byteBufs));
    when(delegate.onClose()).thenReturn(Mono.never());
    when(delegate.alloc()).thenReturn(allocator);

    new ReassemblyDuplexConnection(delegate, false)
        .receive()
        .as(StepVerifier::create)
        .assertNext(
            byteBuf -> {
              System.out.println(byteBuf.readableBytes());
              ByteBuf m = RequestResponseFrameCodec.metadata(byteBuf);
              Assert.assertEquals(metadata, m);
            })
        .verifyComplete();
  }

  @DisplayName("reassembles metadata and data")
  @Test
  void reassembleMetadataAndData() {
    List<ByteBuf> byteBufs =
        Arrays.asList(
            RequestResponseFrameCodec.encode(
                allocator, 1, true, Unpooled.wrappedBuffer(metadata), Unpooled.EMPTY_BUFFER),
            PayloadFrameCodec.encode(
                allocator,
                1,
                true,
                false,
                true,
                Unpooled.wrappedBuffer(metadata),
                Unpooled.EMPTY_BUFFER),
            PayloadFrameCodec.encode(
                allocator,
                1,
                true,
                false,
                true,
                Unpooled.wrappedBuffer(metadata),
                Unpooled.EMPTY_BUFFER),
            PayloadFrameCodec.encode(
                allocator,
                1,
                true,
                false,
                true,
                Unpooled.wrappedBuffer(metadata),
                Unpooled.wrappedBuffer(data)),
            PayloadFrameCodec.encode(
                allocator, 1, false, false, true, null, Unpooled.wrappedBuffer(data)));

    CompositeByteBuf data =
        allocator
            .compositeDirectBuffer()
            .addComponents(
                true,
                Unpooled.wrappedBuffer(ReassembleDuplexConnectionTest.data),
                Unpooled.wrappedBuffer(ReassembleDuplexConnectionTest.data));

    CompositeByteBuf metadata =
        allocator
            .compositeDirectBuffer()
            .addComponents(
                true,
                Unpooled.wrappedBuffer(ReassembleDuplexConnectionTest.metadata),
                Unpooled.wrappedBuffer(ReassembleDuplexConnectionTest.metadata),
                Unpooled.wrappedBuffer(ReassembleDuplexConnectionTest.metadata),
                Unpooled.wrappedBuffer(ReassembleDuplexConnectionTest.metadata));

    when(delegate.receive()).thenReturn(Flux.fromIterable(byteBufs));
    when(delegate.onClose()).thenReturn(Mono.never());
    when(delegate.alloc()).thenReturn(allocator);

    new ReassemblyDuplexConnection(delegate, false)
        .receive()
        .as(StepVerifier::create)
        .assertNext(
            byteBuf -> {
              Assert.assertEquals(data, RequestResponseFrameCodec.data(byteBuf));
              Assert.assertEquals(metadata, RequestResponseFrameCodec.metadata(byteBuf));
            })
        .verifyComplete();
  }

  @DisplayName("does not reassemble a non-fragment frame")
  @Test
  void reassembleNonFragment() {
    ByteBuf encode =
        RequestResponseFrameCodec.encode(allocator, 1, false, null, Unpooled.wrappedBuffer(data));

    when(delegate.receive()).thenReturn(Flux.just(encode));
    when(delegate.onClose()).thenReturn(Mono.never());
    when(delegate.alloc()).thenReturn(allocator);

    new ReassemblyDuplexConnection(delegate, false)
        .receive()
        .as(StepVerifier::create)
        .assertNext(
            byteBuf -> {
              Assert.assertEquals(
                  Unpooled.wrappedBuffer(data), RequestResponseFrameCodec.data(byteBuf));
            })
        .verifyComplete();
  }

  @DisplayName("does not reassemble non fragmentable frame")
  @Test
  void reassembleNonFragmentableFrame() {
    ByteBuf encode = CancelFrameCodec.encode(allocator, 2);

    when(delegate.receive()).thenReturn(Flux.just(encode));
    when(delegate.onClose()).thenReturn(Mono.never());
    when(delegate.alloc()).thenReturn(allocator);

    new ReassemblyDuplexConnection(delegate, false)
        .receive()
        .as(StepVerifier::create)
        .assertNext(
            byteBuf -> {
              Assert.assertEquals(FrameType.CANCEL, FrameHeaderCodec.frameType(byteBuf));
            })
        .verifyComplete();
  }
}
