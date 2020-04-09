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

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.Mockito.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.DuplexConnection;
import io.rsocket.frame.*;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Assert;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

final class FragmentationDuplexConnectionTest {
  private static byte[] data = new byte[1024];
  private static byte[] metadata = new byte[1024];

  static {
    ThreadLocalRandom.current().nextBytes(data);
    ThreadLocalRandom.current().nextBytes(metadata);
  }

  private final DuplexConnection delegate = mock(DuplexConnection.class, RETURNS_SMART_NULLS);

  @SuppressWarnings("unchecked")
  private final ArgumentCaptor<Publisher<ByteBuf>> publishers =
      ArgumentCaptor.forClass(Publisher.class);

  private ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

  @DisplayName("constructor throws IllegalArgumentException with negative maxFragmentLength")
  @Test
  void constructorInvalidMaxFragmentSize() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new FragmentationDuplexConnection(
                    delegate, allocator, Integer.MIN_VALUE, false, ""))
        .withMessage("smallest allowed mtu size is 64 bytes, provided: -2147483648");
  }

  @DisplayName("constructor throws IllegalArgumentException with negative maxFragmentLength")
  @Test
  void constructorMtuLessThanMin() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new FragmentationDuplexConnection(delegate, allocator, 2, false, ""))
        .withMessage("smallest allowed mtu size is 64 bytes, provided: 2");
  }

  @DisplayName("constructor throws NullPointerException with null byteBufAllocator")
  @Test
  void constructorNullByteBufAllocator() {
    assertThatNullPointerException()
        .isThrownBy(() -> new FragmentationDuplexConnection(delegate, null, 64, false, ""))
        .withMessage("byteBufAllocator must not be null");
  }

  @DisplayName("constructor throws NullPointerException with null delegate")
  @Test
  void constructorNullDelegate() {
    assertThatNullPointerException()
        .isThrownBy(() -> new FragmentationDuplexConnection(null, allocator, 64, false, ""))
        .withMessage("delegate must not be null");
  }

  @DisplayName("fragments data")
  @Test
  void sendData() {
    ByteBuf encode =
        RequestResponseFrameFlyweight.encode(
            allocator, 1, false, Unpooled.EMPTY_BUFFER, Unpooled.wrappedBuffer(data));

    when(delegate.onClose()).thenReturn(Mono.never());

    new FragmentationDuplexConnection(delegate, allocator, 64, false, "").sendOne(encode.retain());

    verify(delegate).send(publishers.capture());

    StepVerifier.create(Flux.from(publishers.getValue()))
        .expectNextCount(17)
        .assertNext(
            byteBuf -> {
              Assert.assertEquals(FrameType.NEXT, FrameHeaderFlyweight.frameType(byteBuf));
              Assert.assertFalse(FrameHeaderFlyweight.hasFollows(byteBuf));
            })
        .verifyComplete();
  }
}
