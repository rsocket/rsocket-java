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

package io.rsocket.fragmentation;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import io.rsocket.frame.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Assert;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

final class FrameReassemblerTest {
  private static byte[] data = new byte[1024];
  private static byte[] metadata = new byte[1024];

  static {
    ThreadLocalRandom.current().nextBytes(data);
    ThreadLocalRandom.current().nextBytes(metadata);
  }

  private ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

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

    FrameReassembler reassembler = new FrameReassembler(allocator);

    Flux<ByteBuf> assembled = Flux.fromIterable(byteBufs).handle(reassembler::reassembleFrame);

    CompositeByteBuf data =
        allocator
            .compositeDirectBuffer()
            .addComponents(
                true,
                Unpooled.wrappedBuffer(FrameReassemblerTest.data),
                Unpooled.wrappedBuffer(FrameReassemblerTest.data),
                Unpooled.wrappedBuffer(FrameReassemblerTest.data),
                Unpooled.wrappedBuffer(FrameReassemblerTest.data),
                Unpooled.wrappedBuffer(FrameReassemblerTest.data));

    StepVerifier.create(assembled)
        .assertNext(
            byteBuf -> {
              Assert.assertEquals(data, RequestResponseFrameCodec.data(byteBuf));
              ReferenceCountUtil.safeRelease(byteBuf);
            })
        .verifyComplete();
    ReferenceCountUtil.safeRelease(data);
  }

  @DisplayName("pass through frames without follows")
  @Test
  void passthrough() {
    List<ByteBuf> byteBufs =
        Arrays.asList(
            RequestResponseFrameCodec.encode(
                allocator, 1, false, null, Unpooled.wrappedBuffer(data)));

    FrameReassembler reassembler = new FrameReassembler(allocator);

    Flux<ByteBuf> assembled = Flux.fromIterable(byteBufs).handle(reassembler::reassembleFrame);

    CompositeByteBuf data =
        allocator
            .compositeDirectBuffer()
            .addComponents(true, Unpooled.wrappedBuffer(FrameReassemblerTest.data));

    StepVerifier.create(assembled)
        .assertNext(
            byteBuf -> {
              Assert.assertEquals(data, RequestResponseFrameCodec.data(byteBuf));
              ReferenceCountUtil.safeRelease(byteBuf);
            })
        .verifyComplete();
    ReferenceCountUtil.safeRelease(data);
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

    FrameReassembler reassembler = new FrameReassembler(allocator);

    Flux<ByteBuf> assembled = Flux.fromIterable(byteBufs).handle(reassembler::reassembleFrame);

    CompositeByteBuf metadata =
        allocator
            .compositeDirectBuffer()
            .addComponents(
                true,
                Unpooled.wrappedBuffer(FrameReassemblerTest.metadata),
                Unpooled.wrappedBuffer(FrameReassemblerTest.metadata),
                Unpooled.wrappedBuffer(FrameReassemblerTest.metadata),
                Unpooled.wrappedBuffer(FrameReassemblerTest.metadata),
                Unpooled.wrappedBuffer(FrameReassemblerTest.metadata));

    StepVerifier.create(assembled)
        .assertNext(
            byteBuf -> {
              System.out.println(byteBuf.readableBytes());
              ByteBuf m = RequestResponseFrameCodec.metadata(byteBuf);
              Assert.assertEquals(metadata, m);
            })
        .verifyComplete();
  }

  @DisplayName("reassembles metadata request channel")
  @Test
  void reassembleMetadataChannel() {
    List<ByteBuf> byteBufs =
        Arrays.asList(
            RequestChannelFrameCodec.encode(
                allocator,
                1,
                true,
                false,
                100,
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

    FrameReassembler reassembler = new FrameReassembler(allocator);

    Flux<ByteBuf> assembled = Flux.fromIterable(byteBufs).handle(reassembler::reassembleFrame);

    CompositeByteBuf metadata =
        allocator
            .compositeDirectBuffer()
            .addComponents(
                true,
                Unpooled.wrappedBuffer(FrameReassemblerTest.metadata),
                Unpooled.wrappedBuffer(FrameReassemblerTest.metadata),
                Unpooled.wrappedBuffer(FrameReassemblerTest.metadata),
                Unpooled.wrappedBuffer(FrameReassemblerTest.metadata),
                Unpooled.wrappedBuffer(FrameReassemblerTest.metadata));

    StepVerifier.create(assembled)
        .assertNext(
            byteBuf -> {
              System.out.println(byteBuf.readableBytes());
              ByteBuf m = RequestChannelFrameCodec.metadata(byteBuf);
              Assert.assertEquals(metadata, m);
              Assert.assertEquals(100, RequestChannelFrameCodec.initialRequestN(byteBuf));
              ReferenceCountUtil.safeRelease(byteBuf);
            })
        .verifyComplete();

    ReferenceCountUtil.safeRelease(metadata);
  }

  @DisplayName("reassembles metadata request stream")
  @Test
  void reassembleMetadataStream() {
    List<ByteBuf> byteBufs =
        Arrays.asList(
            RequestStreamFrameCodec.encode(
                allocator, 1, true, 250, Unpooled.wrappedBuffer(metadata), Unpooled.EMPTY_BUFFER),
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

    FrameReassembler reassembler = new FrameReassembler(allocator);

    Flux<ByteBuf> assembled = Flux.fromIterable(byteBufs).handle(reassembler::reassembleFrame);

    CompositeByteBuf metadata =
        allocator
            .compositeDirectBuffer()
            .addComponents(
                true,
                Unpooled.wrappedBuffer(FrameReassemblerTest.metadata),
                Unpooled.wrappedBuffer(FrameReassemblerTest.metadata),
                Unpooled.wrappedBuffer(FrameReassemblerTest.metadata),
                Unpooled.wrappedBuffer(FrameReassemblerTest.metadata),
                Unpooled.wrappedBuffer(FrameReassemblerTest.metadata));

    StepVerifier.create(assembled)
        .assertNext(
            byteBuf -> {
              System.out.println(byteBuf.readableBytes());
              ByteBuf m = RequestStreamFrameCodec.metadata(byteBuf);
              Assert.assertEquals(metadata, m);
              Assert.assertEquals(250, RequestChannelFrameCodec.initialRequestN(byteBuf));
              ReferenceCountUtil.safeRelease(byteBuf);
            })
        .verifyComplete();

    ReferenceCountUtil.safeRelease(metadata);
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

    FrameReassembler reassembler = new FrameReassembler(allocator);

    Flux<ByteBuf> assembled = Flux.fromIterable(byteBufs).handle(reassembler::reassembleFrame);

    CompositeByteBuf data =
        allocator
            .compositeDirectBuffer()
            .addComponents(
                true,
                Unpooled.wrappedBuffer(FrameReassemblerTest.data),
                Unpooled.wrappedBuffer(FrameReassemblerTest.data));

    CompositeByteBuf metadata =
        allocator
            .compositeDirectBuffer()
            .addComponents(
                true,
                Unpooled.wrappedBuffer(FrameReassemblerTest.metadata),
                Unpooled.wrappedBuffer(FrameReassemblerTest.metadata),
                Unpooled.wrappedBuffer(FrameReassemblerTest.metadata),
                Unpooled.wrappedBuffer(FrameReassemblerTest.metadata));

    StepVerifier.create(assembled)
        .assertNext(
            byteBuf -> {
              Assert.assertEquals(data, RequestResponseFrameCodec.data(byteBuf));
              Assert.assertEquals(metadata, RequestResponseFrameCodec.metadata(byteBuf));
            })
        .verifyComplete();
    ReferenceCountUtil.safeRelease(data);
    ReferenceCountUtil.safeRelease(metadata);
  }

  @DisplayName("cancel removes inflight frames")
  @Test
  public void cancelBeforeAssembling() {
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
                Unpooled.wrappedBuffer(data)));

    FrameReassembler reassembler = new FrameReassembler(allocator);
    Flux.fromIterable(byteBufs).handle(reassembler::reassembleFrame).blockLast();

    Assert.assertTrue(reassembler.headers.containsKey(1));
    Assert.assertTrue(reassembler.metadata.containsKey(1));
    Assert.assertTrue(reassembler.data.containsKey(1));

    Flux.just(CancelFrameCodec.encode(allocator, 1))
        .handle(reassembler::reassembleFrame)
        .blockLast();

    Assert.assertFalse(reassembler.headers.containsKey(1));
    Assert.assertFalse(reassembler.metadata.containsKey(1));
    Assert.assertFalse(reassembler.data.containsKey(1));
  }

  @DisplayName("dispose should clean up maps")
  @Test
  public void dispose() {
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
                Unpooled.wrappedBuffer(data)));

    FrameReassembler reassembler = new FrameReassembler(allocator);
    Flux.fromIterable(byteBufs).handle(reassembler::reassembleFrame).blockLast();

    Assert.assertTrue(reassembler.headers.containsKey(1));
    Assert.assertTrue(reassembler.metadata.containsKey(1));
    Assert.assertTrue(reassembler.data.containsKey(1));

    reassembler.dispose();

    Assert.assertFalse(reassembler.headers.containsKey(1));
    Assert.assertFalse(reassembler.metadata.containsKey(1));
    Assert.assertFalse(reassembler.data.containsKey(1));
  }
}
