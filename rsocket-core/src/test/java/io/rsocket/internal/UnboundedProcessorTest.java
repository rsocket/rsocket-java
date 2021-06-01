/*
 * Copyright 2015-present the original author or authors.
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
import io.rsocket.RaceTestConstants;
import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
import io.rsocket.internal.subscriber.AssertSubscriber;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.Fuseable;
import reactor.core.publisher.Hooks;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.util.RaceTestUtils;

public class UnboundedProcessorTest {

  @BeforeAll
  public static void setup() {
    Hooks.onErrorDropped(__ -> {});
  }

  @AfterAll
  public static void teardown() {
    Hooks.resetOnErrorDropped();
  }

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

    for (int i = 0; i < RaceTestConstants.REPEATS; i++) {
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
    for (int i = 0; i < RaceTestConstants.REPEATS; i++) {
      final UnboundedProcessor unboundedProcessor = new UnboundedProcessor();

      final ByteBuf buffer1 = allocator.buffer(1);
      final ByteBuf buffer2 = allocator.buffer(2);

      final AssertSubscriber<ByteBuf> assertSubscriber =
          new AssertSubscriber<ByteBuf>(0)
              .requestedFusionMode(withFusionEnabled ? Fuseable.ANY : Fuseable.NONE);

      unboundedProcessor.subscribe(assertSubscriber);

      RaceTestUtils.race(
          () -> {
            unboundedProcessor.onNext(buffer1);
            unboundedProcessor.onNext(buffer2);
          },
          unboundedProcessor::dispose,
          assertSubscriber::cancel,
          () -> {
            assertSubscriber.request(1);
            assertSubscriber.request(1);
          });

      assertSubscriber.values().forEach(ReferenceCountUtil::release);

      allocator.assertHasNoLeaks();
    }
  }

  @ParameterizedTest(
      name =
          "Ensures that racing between onNext | dispose | cancel | request(n) | terminal will not cause any issues and leaks; mode[fusionEnabled={0}]")
  @ValueSource(booleans = {true, false})
  public void smokeTest1(boolean withFusionEnabled) {
    final LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    final RuntimeException runtimeException = new RuntimeException("test");
    for (int i = 0; i < RaceTestConstants.REPEATS; i++) {
      final UnboundedProcessor unboundedProcessor = new UnboundedProcessor();

      final ByteBuf buffer1 = allocator.buffer(1);
      final ByteBuf buffer2 = allocator.buffer(2);
      final ByteBuf buffer3 = allocator.buffer(3);
      final ByteBuf buffer4 = allocator.buffer(4);

      final AssertSubscriber<ByteBuf> assertSubscriber =
          new AssertSubscriber<ByteBuf>(0)
              .requestedFusionMode(withFusionEnabled ? Fuseable.ANY : Fuseable.NONE);

      unboundedProcessor.subscribe(assertSubscriber);

      RaceTestUtils.race(
          () -> {
            unboundedProcessor.onNext(buffer1);
            unboundedProcessor.onNextPrioritized(buffer2);
          },
          () -> {
            unboundedProcessor.onNextPrioritized(buffer3);
            unboundedProcessor.onNext(buffer4);
          },
          unboundedProcessor::dispose,
          unboundedProcessor::onComplete,
          () -> unboundedProcessor.onError(runtimeException),
          assertSubscriber::cancel,
          () -> {
            assertSubscriber.request(1);
            assertSubscriber.request(1);
            assertSubscriber.request(1);
            assertSubscriber.request(1);
          });

      assertSubscriber.values().forEach(ReferenceCountUtil::release);

      allocator.assertHasNoLeaks();
    }
  }

  @ParameterizedTest(
      name =
          "Ensures that racing between onNext | dispose | subscribe | request(n) | terminal will not cause any issues and leaks; mode[fusionEnabled={0}]")
  @ValueSource(booleans = {true, false})
  public void smokeTest2(boolean withFusionEnabled) {
    final LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    final RuntimeException runtimeException = new RuntimeException("test");
    for (int i = 0; i < RaceTestConstants.REPEATS; i++) {
      final UnboundedProcessor unboundedProcessor = new UnboundedProcessor();

      final ByteBuf buffer1 = allocator.buffer(1);
      final ByteBuf buffer2 = allocator.buffer(2);
      final ByteBuf buffer3 = allocator.buffer(3);
      final ByteBuf buffer4 = allocator.buffer(4);

      final AssertSubscriber<ByteBuf> assertSubscriber =
          new AssertSubscriber<ByteBuf>(0)
              .requestedFusionMode(withFusionEnabled ? Fuseable.ANY : Fuseable.NONE);

      RaceTestUtils.race(
          Schedulers.boundedElastic(),
          () -> {
            unboundedProcessor.onNext(buffer1);
            unboundedProcessor.onNextPrioritized(buffer2);
          },
          () -> {
            unboundedProcessor.onNextPrioritized(buffer3);
            unboundedProcessor.onNext(buffer4);
          },
          unboundedProcessor::dispose,
          unboundedProcessor::onComplete,
          () -> unboundedProcessor.onError(runtimeException),
          () -> {
            unboundedProcessor.subscribe(assertSubscriber);
            assertSubscriber.request(1);
            assertSubscriber.request(1);
            assertSubscriber.request(1);
            assertSubscriber.request(1);
          });

      assertSubscriber.values().forEach(ReferenceCountUtil::release);

      allocator.assertHasNoLeaks();
    }
  }

  @ParameterizedTest(
      name =
          "Ensures that racing between onNext | dispose | subscribe(cancelled) | terminal will not cause any issues and leaks; mode[fusionEnabled={0}]")
  @ValueSource(booleans = {true, false})
  public void smokeTest3(boolean withFusionEnabled) {
    final LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    final RuntimeException runtimeException = new RuntimeException("test");
    for (int i = 0; i < RaceTestConstants.REPEATS; i++) {
      final UnboundedProcessor unboundedProcessor = new UnboundedProcessor();

      final ByteBuf buffer1 = allocator.buffer(1);
      final ByteBuf buffer2 = allocator.buffer(2);
      final ByteBuf buffer3 = allocator.buffer(3);
      final ByteBuf buffer4 = allocator.buffer(4);

      final AssertSubscriber<ByteBuf> assertSubscriber =
          new AssertSubscriber<ByteBuf>(0)
              .requestedFusionMode(withFusionEnabled ? Fuseable.ANY : Fuseable.NONE);

      assertSubscriber.cancel();

      RaceTestUtils.race(
          Schedulers.boundedElastic(),
          () -> {
            unboundedProcessor.onNext(buffer1);
            unboundedProcessor.onNextPrioritized(buffer2);
          },
          () -> {
            unboundedProcessor.onNextPrioritized(buffer3);
            unboundedProcessor.onNext(buffer4);
          },
          unboundedProcessor::dispose,
          unboundedProcessor::onComplete,
          () -> unboundedProcessor.onError(runtimeException),
          () -> unboundedProcessor.subscribe(assertSubscriber));

      assertSubscriber.values().forEach(ReferenceCountUtil::release);

      allocator.assertHasNoLeaks();
    }
  }

  @ParameterizedTest(
      name =
          "Ensures that racing between onNext | dispose | subscribe(cancelled) | terminal will not cause any issues and leaks; mode[fusionEnabled={0}]")
  @ValueSource(booleans = {true, false})
  public void smokeTest31(boolean withFusionEnabled) {
    final LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    final RuntimeException runtimeException = new RuntimeException("test");
    for (int i = 0; i < RaceTestConstants.REPEATS; i++) {
      final UnboundedProcessor unboundedProcessor = new UnboundedProcessor();

      final ByteBuf buffer1 = allocator.buffer(1);
      final ByteBuf buffer2 = allocator.buffer(2);
      final ByteBuf buffer3 = allocator.buffer(3);
      final ByteBuf buffer4 = allocator.buffer(4);

      final AssertSubscriber<ByteBuf> assertSubscriber =
          new AssertSubscriber<ByteBuf>(0)
              .requestedFusionMode(withFusionEnabled ? Fuseable.ANY : Fuseable.NONE);

      RaceTestUtils.race(
          Schedulers.boundedElastic(),
          () -> {
            unboundedProcessor.onNext(buffer1);
            unboundedProcessor.onNextPrioritized(buffer2);
          },
          () -> {
            unboundedProcessor.onNextPrioritized(buffer3);
            unboundedProcessor.onNext(buffer4);
          },
          unboundedProcessor::dispose,
          unboundedProcessor::onComplete,
          () -> unboundedProcessor.onError(runtimeException),
          () -> unboundedProcessor.subscribe(assertSubscriber),
          () -> {
            assertSubscriber.request(1);
            assertSubscriber.request(1);
            assertSubscriber.request(1);
            assertSubscriber.request(1);
          },
          assertSubscriber::cancel);

      assertSubscriber.values().forEach(ReferenceCountUtil::release);
      allocator.assertHasNoLeaks();
    }
  }

  @ParameterizedTest(
      name =
          "Ensures that racing between onNext + dispose | downstream async drain should not cause any issues and leaks; mode[fusionEnabled={0}]")
  @ValueSource(booleans = {true, false})
  public void ensuresAsyncFusionAndDisposureHasNoDeadlock(boolean withFusionEnabled) {
    final LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);

    for (int i = 0; i < RaceTestConstants.REPEATS; i++) {
      final UnboundedProcessor unboundedProcessor = new UnboundedProcessor();
      final ByteBuf buffer1 = allocator.buffer(1);
      final ByteBuf buffer2 = allocator.buffer(2);
      final ByteBuf buffer3 = allocator.buffer(3);
      final ByteBuf buffer4 = allocator.buffer(4);
      final ByteBuf buffer5 = allocator.buffer(5);
      final ByteBuf buffer6 = allocator.buffer(6);

      final AssertSubscriber<ByteBuf> assertSubscriber =
          new AssertSubscriber<ByteBuf>()
              .requestedFusionMode(withFusionEnabled ? Fuseable.ANY : Fuseable.NONE);

      unboundedProcessor.subscribe(assertSubscriber);

      RaceTestUtils.race(
          () -> {
            unboundedProcessor.onNext(buffer1);
            unboundedProcessor.onNext(buffer2);
            unboundedProcessor.onNext(buffer3);
            unboundedProcessor.onNext(buffer4);
            unboundedProcessor.onNext(buffer5);
            unboundedProcessor.onNext(buffer6);
            unboundedProcessor.dispose();
          },
          unboundedProcessor::dispose);

      assertSubscriber.await(Duration.ofSeconds(50)).values().forEach(ReferenceCountUtil::release);
    }

    allocator.assertHasNoLeaks();
  }
}
