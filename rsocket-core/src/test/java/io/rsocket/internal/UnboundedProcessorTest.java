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

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
import io.rsocket.internal.subscriber.AssertSubscriber;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.Fuseable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.util.RaceTestUtils;

public class UnboundedProcessorTest {

  @ParameterizedTest(
      name =
          "Test that emitting {0} onNext before subscribe and requestN should deliver all the signals once the subscriber is available")
  @ValueSource(ints = {10, 100, 10_000, 100_000, 1_000_000, 10_000_000})
  public void testOnNextBeforeSubscribeN(int n) {
    UnboundedProcessor processor = new UnboundedProcessor();

    for (int i = 0; i < n; i++) {
      processor.onNext(Unpooled.EMPTY_BUFFER);
    }

    processor.onComplete();

    StepVerifier.create(processor.count()).expectNext(Long.valueOf(n)).verifyComplete();
  }

  @ParameterizedTest(
      name =
          "Test that emitting {0} onNext after subscribe and requestN should deliver all the signals")
  @ValueSource(ints = {10, 100, 10_000})
  public void testOnNextAfterSubscribeN(int n) {
    UnboundedProcessor processor = new UnboundedProcessor();
    AssertSubscriber<ByteBuf> assertSubscriber = AssertSubscriber.create();

    processor.subscribe(assertSubscriber);

    for (int i = 0; i < n; i++) {
      processor.onNext(Unpooled.EMPTY_BUFFER);
    }

    assertSubscriber.awaitAndAssertNextValueCount(n);
  }

  @ParameterizedTest(
      name =
          "Test that prioritized value sending deliver prioritized signals before the others mode[fusionEnabled={0}]")
  @ValueSource(booleans = {true, false})
  public void testPrioritizedSending(boolean fusedCase) {
    UnboundedProcessor processor = new UnboundedProcessor();

    for (int i = 0; i < 1000; i++) {
      processor.onNext(Unpooled.EMPTY_BUFFER);
    }

    processor.onNextPrioritized(Unpooled.copiedBuffer("test", CharsetUtil.UTF_8));

    assertThat(fusedCase ? processor.poll() : processor.next().block())
        .isNotNull()
        .extracting(bb -> bb.toString(CharsetUtil.UTF_8))
        .isEqualTo("test");
  }

  @ParameterizedTest(
      name =
          "Ensures that racing between onNext | dispose | cancel | request(n) will not cause any issues and leaks; mode[fusionEnabled={0}]")
  @ValueSource(booleans = {true, false})
  public void ensureUnboundedProcessorDisposesQueueProperly(boolean withFusionEnabled) {
    final LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    for (int i = 0; i < 10000; i++) {
      final UnboundedProcessor unboundedProcessor = new UnboundedProcessor();

      final ByteBuf buffer1 = allocator.buffer(1);
      final ByteBuf buffer2 = allocator.buffer(2);

      final AssertSubscriber<ByteBuf> assertSubscriber =
          new AssertSubscriber<ByteBuf>(0)
              .requestedFusionMode(withFusionEnabled ? Fuseable.ANY : Fuseable.NONE);

      unboundedProcessor.subscribe(assertSubscriber);

      RaceTestUtils.race(
          () ->
              RaceTestUtils.race(
                  () ->
                      RaceTestUtils.race(
                          () -> {
                            unboundedProcessor.onNext(buffer1);
                            unboundedProcessor.onNext(buffer2);
                          },
                          unboundedProcessor::dispose,
                          Schedulers.elastic()),
                  assertSubscriber::cancel,
                  Schedulers.elastic()),
          () -> {
            assertSubscriber.request(1);
            assertSubscriber.request(1);
          },
          Schedulers.elastic());

      assertSubscriber.values().forEach(ReferenceCountUtil::safeRelease);

      allocator.assertHasNoLeaks();
    }
  }
}
