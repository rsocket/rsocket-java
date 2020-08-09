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
package io.rsocket.core;

import static io.rsocket.core.FragmentationUtils.FRAME_OFFSET;
import static io.rsocket.core.FragmentationUtils.FRAME_OFFSET_WITH_METADATA;
import static io.rsocket.core.PayloadValidationUtils.INVALID_PAYLOAD_ERROR_MESSAGE;
import static io.rsocket.core.TestRequesterResponderSupport.genericPayload;
import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.IllegalReferenceCountException;
import io.rsocket.FrameAssert;
import io.rsocket.Payload;
import io.rsocket.PayloadAssert;
import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
import io.rsocket.exceptions.ApplicationErrorException;
import io.rsocket.frame.FrameType;
import io.rsocket.test.util.TestDuplexConnection;
import io.rsocket.util.ByteBufPayload;
import io.rsocket.util.EmptyPayload;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.Scannable;
import reactor.test.StepVerifier;

public class RequestResponseRequesterMonoTest {

  @BeforeAll
  public static void setUp() {
    StepVerifier.setDefaultTimeout(Duration.ofSeconds(2));
  }

  /*
   * +-------------------------------+
   * |      General Test Cases       |
   * +-------------------------------+
   *
   */

  /**
   * General StateMachine transition test. No Fragmentation enabled In this test we check that the
   * given instance of RequestResponseMono: 1) subscribes 2) sends frame on the first request 3)
   * terminates up on receiving the first signal (terminates on first next | error | next over
   * reassembly | complete)
   */
  @ParameterizedTest
  @MethodSource("frameShouldBeSentOnSubscriptionResponses")
  public void frameShouldBeSentOnSubscription(
      BiFunction<RequestResponseRequesterMono, StepVerifier.Step<Payload>, StepVerifier>
          transformer) {
    final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
    final LeaksTrackingByteBufAllocator allocator = activeStreams.getAllocator();
    final TestDuplexConnection sender = activeStreams.getDuplexConnection();
    final Payload payload = genericPayload(allocator);

    final RequestResponseRequesterMono requestResponseRequesterMono =
        new RequestResponseRequesterMono(payload, activeStreams);

    final StateAssert<RequestResponseRequesterMono> stateAssert =
        StateAssert.assertThat(RequestResponseRequesterMono.STATE, requestResponseRequesterMono);

    stateAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    transformer
        .apply(
            requestResponseRequesterMono,
            StepVerifier.create(requestResponseRequesterMono, 0)
                .expectSubscription()
                .then(stateAssert::hasSubscribedFlagOnly)
                .then(() -> Assertions.assertThat(payload.refCnt()).isOne())
                .then(activeStreams::assertNoActiveStreams)
                .thenRequest(1)
                .then(() -> stateAssert.hasSubscribedFlag().hasRequestN(1).hasFirstFrameSentFlag())
                .then(() -> Assertions.assertThat(payload.refCnt()).isZero())
                .then(() -> activeStreams.assertHasStream(1, requestResponseRequesterMono)))
        .verify();

    PayloadAssert.assertThat(payload).isReleased();
    // should not add anything to map
    activeStreams.assertNoActiveStreams();

    final ByteBuf frame = sender.awaitFrame();
    FrameAssert.assertThat(frame)
        .isNotNull()
        .hasPayloadSize(
            "testData".getBytes(CharsetUtil.UTF_8).length
                + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
        .hasMetadata("testMetadata")
        .hasData("testData")
        .hasNoFragmentsFollow()
        .typeOf(FrameType.REQUEST_RESPONSE)
        .hasClientSideStreamId()
        .hasStreamId(1)
        .hasNoLeaks();

    stateAssert.isTerminated();

    if (!sender.isEmpty()) {
      ByteBuf cancelFrame = sender.awaitFrame();
      FrameAssert.assertThat(cancelFrame)
          .isNotNull()
          .typeOf(FrameType.CANCEL)
          .hasClientSideStreamId()
          .hasStreamId(1)
          .hasNoLeaks();
    }
    Assertions.assertThat(sender.isEmpty()).isTrue();
    allocator.assertHasNoLeaks();
  }

  static Stream<BiFunction<RequestResponseRequesterMono, StepVerifier.Step<Payload>, StepVerifier>>
      frameShouldBeSentOnSubscriptionResponses() {
    return Stream.of(
        // next case
        (rrm, sv) ->
            sv.then(() -> rrm.handlePayload(EmptyPayload.INSTANCE))
                .expectNext(EmptyPayload.INSTANCE)
                .expectComplete(),
        // complete case
        (rrm, sv) -> sv.then(rrm::handleComplete).expectComplete(),
        // error case
        (rrm, sv) ->
            sv.then(() -> rrm.handleError(new ApplicationErrorException("test")))
                .expectErrorSatisfies(
                    t ->
                        Assertions.assertThat(t)
                            .hasMessage("test")
                            .isInstanceOf(ApplicationErrorException.class)),
        // fragmentation case
        (rrm, sv) -> {
          final byte[] metadata = new byte[65];
          final byte[] data = new byte[129];
          ThreadLocalRandom.current().nextBytes(metadata);
          ThreadLocalRandom.current().nextBytes(data);

          final Payload payload = ByteBufPayload.create(data, metadata);
          StateAssert<RequestResponseRequesterMono> stateAssert = StateAssert.assertThat(rrm);

          return sv.then(
                  () -> {
                    final ByteBuf followingFrame =
                        FragmentationUtils.encodeFirstFragment(
                            rrm.allocator,
                            64,
                            FrameType.REQUEST_RESPONSE,
                            1,
                            payload.hasMetadata(),
                            payload.metadata(),
                            payload.data());
                    rrm.handleNext(followingFrame, true, false);
                    followingFrame.release();
                  })
              .then(
                  () ->
                      stateAssert
                          .hasSubscribedFlag()
                          .hasRequestN(1)
                          .hasFirstFrameSentFlag()
                          .hasReassemblingFlag())
              .then(
                  () -> {
                    final ByteBuf followingFrame =
                        FragmentationUtils.encodeFollowsFragment(
                            rrm.allocator, 64, 1, false, payload.metadata(), payload.data());
                    rrm.handleNext(followingFrame, true, false);
                    followingFrame.release();
                  })
              .then(
                  () ->
                      stateAssert
                          .hasSubscribedFlag()
                          .hasRequestN(1)
                          .hasFirstFrameSentFlag()
                          .hasReassemblingFlag())
              .then(
                  () -> {
                    final ByteBuf followingFrame =
                        FragmentationUtils.encodeFollowsFragment(
                            rrm.allocator, 64, 1, false, payload.metadata(), payload.data());
                    rrm.handleNext(followingFrame, true, false);
                    followingFrame.release();
                  })
              .then(
                  () ->
                      stateAssert
                          .hasSubscribedFlag()
                          .hasRequestN(1)
                          .hasFirstFrameSentFlag()
                          .hasReassemblingFlag())
              .then(
                  () -> {
                    final ByteBuf followingFrame =
                        FragmentationUtils.encodeFollowsFragment(
                            rrm.allocator, 64, 1, false, payload.metadata(), payload.data());
                    rrm.handleNext(followingFrame, false, false);
                    followingFrame.release();
                  })
              .then(stateAssert::isTerminated)
              .assertNext(
                  p -> {
                    Assertions.assertThat(p.data()).isEqualTo(Unpooled.wrappedBuffer(data));

                    Assertions.assertThat(p.metadata()).isEqualTo(Unpooled.wrappedBuffer(metadata));
                    p.release();
                  })
              .then(payload::release)
              .expectComplete();
        },
        (rrm, sv) -> {
          final byte[] metadata = new byte[65];
          final byte[] data = new byte[129];
          ThreadLocalRandom.current().nextBytes(metadata);
          ThreadLocalRandom.current().nextBytes(data);

          final Payload payload = ByteBufPayload.create(data, metadata);
          StateAssert<RequestResponseRequesterMono> stateAssert = StateAssert.assertThat(rrm);

          ByteBuf[] fragments =
              new ByteBuf[] {
                FragmentationUtils.encodeFirstFragment(
                    rrm.allocator,
                    64,
                    FrameType.REQUEST_RESPONSE,
                    1,
                    payload.hasMetadata(),
                    payload.metadata(),
                    payload.data()),
                FragmentationUtils.encodeFollowsFragment(
                    rrm.allocator, 64, 1, false, payload.metadata(), payload.data()),
                FragmentationUtils.encodeFollowsFragment(
                    rrm.allocator, 64, 1, false, payload.metadata(), payload.data())
              };

          final StepVerifier stepVerifier =
              sv.then(
                      () -> {
                        rrm.handleNext(fragments[0], true, false);
                        fragments[0].release();
                      })
                  .then(
                      () ->
                          stateAssert
                              .hasSubscribedFlag()
                              .hasRequestN(1)
                              .hasFirstFrameSentFlag()
                              .hasReassemblingFlag())
                  .then(
                      () -> {
                        rrm.handleNext(fragments[1], true, false);
                        fragments[1].release();
                      })
                  .then(
                      () ->
                          stateAssert
                              .hasSubscribedFlag()
                              .hasRequestN(1)
                              .hasFirstFrameSentFlag()
                              .hasReassemblingFlag())
                  .then(
                      () -> {
                        rrm.handleNext(fragments[2], true, false);
                        fragments[2].release();
                      })
                  .then(
                      () ->
                          stateAssert
                              .hasSubscribedFlag()
                              .hasRequestN(1)
                              .hasFirstFrameSentFlag()
                              .hasReassemblingFlag())
                  .then(payload::release)
                  .thenCancel()
                  .verifyLater();

          stepVerifier.verify();

          Assertions.assertThat(fragments).allMatch(bb -> bb.refCnt() == 0);

          return stepVerifier;
        });
  }

  /**
   * General StateMachine transition test. Fragmentation enabled In this test we check that the
   * given instance of RequestResponseMono: 1) subscribes 2) sends fragments frames on the first
   * request 3) terminates up on receiving the first signal (terminates on first next | error | next
   * over reassembly | complete)
   */
  @ParameterizedTest
  @MethodSource("frameShouldBeSentOnSubscriptionResponses")
  public void frameFragmentsShouldBeSentOnSubscription(
      BiFunction<RequestResponseRequesterMono, StepVerifier.Step<Payload>, StepVerifier>
          transformer) {
    final int mtu = 64;
    final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client(mtu);
    final LeaksTrackingByteBufAllocator allocator = activeStreams.getAllocator();
    final TestDuplexConnection sender = activeStreams.getDuplexConnection();

    final byte[] metadata = new byte[65];
    final byte[] data = new byte[129];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);

    final Payload payload = ByteBufPayload.create(data, metadata);

    final RequestResponseRequesterMono requestResponseRequesterMono =
        new RequestResponseRequesterMono(payload, activeStreams);
    final StateAssert<RequestResponseRequesterMono> stateAssert =
        StateAssert.assertThat(requestResponseRequesterMono);

    stateAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    transformer
        .apply(
            requestResponseRequesterMono,
            StepVerifier.create(requestResponseRequesterMono, 0)
                .expectSubscription()
                .then(stateAssert::hasSubscribedFlagOnly)
                .then(() -> Assertions.assertThat(payload.refCnt()).isOne())
                .then(activeStreams::assertNoActiveStreams)
                .thenRequest(1)
                .then(() -> stateAssert.hasSubscribedFlag().hasRequestN(1).hasFirstFrameSentFlag())
                .then(() -> Assertions.assertThat(payload.refCnt()).isZero())
                .then(() -> activeStreams.assertHasStream(1, requestResponseRequesterMono)))
        .verify();

    // should not add anything to map
    activeStreams.assertNoActiveStreams();

    Assertions.assertThat(payload.refCnt()).isZero();

    final ByteBuf frameFragment1 = sender.awaitFrame();
    FrameAssert.assertThat(frameFragment1)
        .isNotNull()
        .hasPayloadSize(
            64 - FRAME_OFFSET_WITH_METADATA) // 64 - 6 (frame headers) - 3 (encoded metadata
        // length) - 3 frame length
        .hasMetadata(Arrays.copyOf(metadata, 52))
        .hasData(Unpooled.EMPTY_BUFFER)
        .hasFragmentsFollow()
        .typeOf(FrameType.REQUEST_RESPONSE)
        .hasClientSideStreamId()
        .hasStreamId(1)
        .hasNoLeaks();

    final ByteBuf frameFragment2 = sender.awaitFrame();
    FrameAssert.assertThat(frameFragment2)
        .isNotNull()
        .hasPayloadSize(
            64 - FRAME_OFFSET_WITH_METADATA) // 64 - 6 (frame headers) - 3 (encoded metadata
        // length) - 3 frame length
        .hasMetadata(Arrays.copyOfRange(metadata, 52, 65))
        .hasData(Arrays.copyOf(data, 39))
        .hasFragmentsFollow()
        .typeOf(FrameType.NEXT)
        .hasClientSideStreamId()
        .hasStreamId(1)
        .hasNoLeaks();

    final ByteBuf frameFragment3 = sender.awaitFrame();
    FrameAssert.assertThat(frameFragment3)
        .isNotNull()
        .hasPayloadSize(
            64 - FRAME_OFFSET) // 64 - 6 (frame headers) - 3 frame length (no metadata - no length)
        .hasNoMetadata()
        .hasData(Arrays.copyOfRange(data, 39, 94))
        .hasFragmentsFollow()
        .typeOf(FrameType.NEXT)
        .hasClientSideStreamId()
        .hasStreamId(1)
        .hasNoLeaks();

    final ByteBuf frameFragment4 = sender.awaitFrame();
    FrameAssert.assertThat(frameFragment4)
        .isNotNull()
        .hasPayloadSize(35)
        .hasNoMetadata()
        .hasData(Arrays.copyOfRange(data, 94, 129))
        .hasNoFragmentsFollow()
        .typeOf(FrameType.NEXT)
        .hasClientSideStreamId()
        .hasStreamId(1)
        .hasNoLeaks();

    if (!sender.isEmpty()) {
      FrameAssert.assertThat(sender.awaitFrame())
          .isNotNull()
          .typeOf(FrameType.CANCEL)
          .hasClientSideStreamId()
          .hasStreamId(1)
          .hasNoLeaks();
    }
    Assertions.assertThat(sender.isEmpty()).isTrue();
    stateAssert.isTerminated();
    allocator.assertHasNoLeaks();
  }

  /**
   * General StateMachine transition test. Ensures that no fragment is sent if mono was cancelled
   * before any requests
   */
  @Test
  public void shouldBeNoOpsOnCancel() {
    final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
    final LeaksTrackingByteBufAllocator allocator = activeStreams.getAllocator();
    final TestDuplexConnection sender = activeStreams.getDuplexConnection();
    final Payload payload = ByteBufPayload.create("testData", "testMetadata");

    final RequestResponseRequesterMono requestResponseRequesterMono =
        new RequestResponseRequesterMono(payload, activeStreams);
    final StateAssert<RequestResponseRequesterMono> stateAssert =
        StateAssert.assertThat(requestResponseRequesterMono);

    activeStreams.assertNoActiveStreams();
    stateAssert.isUnsubscribed();

    StepVerifier.create(requestResponseRequesterMono, 0)
        .expectSubscription()
        .then(() -> stateAssert.hasSubscribedFlagOnly())
        .then(() -> activeStreams.assertNoActiveStreams())
        .thenCancel()
        .verify();

    Assertions.assertThat(payload.refCnt()).isZero();

    activeStreams.assertNoActiveStreams();
    Assertions.assertThat(sender.isEmpty()).isTrue();
    stateAssert.isTerminated();
    allocator.assertHasNoLeaks();
  }

  /**
   * General state machine test Ensures that a Subscriber receives error signal and state migrate to
   * the terminated in case the given payload is an invalid one.
   */
  @ParameterizedTest
  @MethodSource("shouldErrorOnIncorrectRefCntInGivenPayloadSource")
  public void shouldErrorOnIncorrectRefCntInGivenPayload(
      Consumer<RequestResponseRequesterMono> monoConsumer) {
    final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
    final LeaksTrackingByteBufAllocator allocator = activeStreams.getAllocator();
    final TestDuplexConnection sender = activeStreams.getDuplexConnection();
    ;
    final Payload payload = ByteBufPayload.create("");
    payload.release();

    final RequestResponseRequesterMono requestResponseRequesterMono =
        new RequestResponseRequesterMono(payload, activeStreams);

    final StateAssert<RequestResponseRequesterMono> stateAssert =
        StateAssert.assertThat(requestResponseRequesterMono);

    stateAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    monoConsumer.accept(requestResponseRequesterMono);

    stateAssert.isTerminated();
    activeStreams.assertNoActiveStreams();
    Assertions.assertThat(sender.isEmpty()).isTrue();
    allocator.assertHasNoLeaks();
  }

  static Stream<Consumer<RequestResponseRequesterMono>>
      shouldErrorOnIncorrectRefCntInGivenPayloadSource() {
    return Stream.of(
        (s) ->
            StepVerifier.create(s)
                .expectSubscription()
                .expectError(IllegalReferenceCountException.class)
                .verify(),
        requestResponseRequesterMono ->
            Assertions.assertThatThrownBy(requestResponseRequesterMono::block)
                .isInstanceOf(IllegalReferenceCountException.class));
  }

  /**
   * General state machine test Ensures that a Subscriber receives error signal and state migrate to
   * the terminated in case the given payload was release in the middle of interaction.
   * Fragmentation is disabled
   */
  @Test
  public void shouldErrorOnIncorrectRefCntInGivenPayloadLatePhase() {
    final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
    final LeaksTrackingByteBufAllocator allocator = activeStreams.getAllocator();
    final TestDuplexConnection sender = activeStreams.getDuplexConnection();
    ;
    final Payload payload = ByteBufPayload.create("");

    final RequestResponseRequesterMono requestResponseRequesterMono =
        new RequestResponseRequesterMono(payload, activeStreams);
    final StateAssert<RequestResponseRequesterMono> stateAssert =
        StateAssert.assertThat(requestResponseRequesterMono);

    stateAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    StepVerifier.create(requestResponseRequesterMono, 0)
        .expectSubscription()
        .then(payload::release)
        .thenRequest(1)
        .expectError(IllegalReferenceCountException.class)
        .verify();

    activeStreams.assertNoActiveStreams();
    Assertions.assertThat(sender.isEmpty()).isTrue();
    stateAssert.isTerminated();
    allocator.assertHasNoLeaks();
  }

  /**
   * General state machine test Ensures that a Subscriber receives error signal and state migrate to
   * the terminated in case the given payload was release in the middle of interaction.
   * Fragmentation is enabled
   */
  @Test
  public void shouldErrorOnIncorrectRefCntInGivenPayloadLatePhaseWithFragmentation() {
    final int mtu = 64;
    final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client(mtu);
    final LeaksTrackingByteBufAllocator allocator = activeStreams.getAllocator();
    final TestDuplexConnection sender = activeStreams.getDuplexConnection();
    ;
    final byte[] metadata = new byte[65];
    final byte[] data = new byte[129];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);

    final Payload payload = ByteBufPayload.create(data, metadata);

    final RequestResponseRequesterMono requestResponseRequesterMono =
        new RequestResponseRequesterMono(payload, activeStreams);
    final StateAssert<RequestResponseRequesterMono> stateAssert =
        StateAssert.assertThat(requestResponseRequesterMono);

    stateAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    StepVerifier.create(requestResponseRequesterMono, 0)
        .expectSubscription()
        .then(payload::release)
        .thenRequest(1)
        .expectError(IllegalReferenceCountException.class)
        .verify();

    activeStreams.assertNoActiveStreams();
    Assertions.assertThat(sender.isEmpty()).isTrue();
    stateAssert.isTerminated();
    allocator.assertHasNoLeaks();
  }

  /**
   * General state machine test Ensures that a Subscriber receives error signal and state migrates
   * to the terminated in case the given payload is too big with disabled fragmentation
   */
  @ParameterizedTest
  @MethodSource("shouldErrorIfFragmentExitsAllowanceIfFragmentationDisabledSource")
  public void shouldErrorIfFragmentExitsAllowanceIfFragmentationDisabled(
      Consumer<RequestResponseRequesterMono> monoConsumer) {
    final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
    final LeaksTrackingByteBufAllocator allocator = activeStreams.getAllocator();
    final TestDuplexConnection sender = activeStreams.getDuplexConnection();
    ;

    final byte[] metadata = new byte[FRAME_LENGTH_MASK];
    final byte[] data = new byte[FRAME_LENGTH_MASK];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);

    final Payload payload = ByteBufPayload.create(data, metadata);

    final RequestResponseRequesterMono requestResponseRequesterMono =
        new RequestResponseRequesterMono(payload, activeStreams);
    final StateAssert<RequestResponseRequesterMono> stateAssert =
        StateAssert.assertThat(requestResponseRequesterMono);

    activeStreams.assertNoActiveStreams();
    stateAssert.isUnsubscribed();

    monoConsumer.accept(requestResponseRequesterMono);

    Assertions.assertThat(payload.refCnt()).isZero();

    activeStreams.assertNoActiveStreams();
    Assertions.assertThat(sender.isEmpty()).isTrue();
    stateAssert.isTerminated();
    allocator.assertHasNoLeaks();
  }

  static Stream<Consumer<RequestResponseRequesterMono>>
      shouldErrorIfFragmentExitsAllowanceIfFragmentationDisabledSource() {
    return Stream.of(
        (s) ->
            StepVerifier.create(s)
                .expectSubscription()
                .consumeErrorWith(
                    t ->
                        Assertions.assertThat(t)
                            .hasMessage(
                                String.format(INVALID_PAYLOAD_ERROR_MESSAGE, FRAME_LENGTH_MASK))
                            .isInstanceOf(IllegalArgumentException.class))
                .verify(),
        requestResponseRequesterMono ->
            Assertions.assertThatThrownBy(requestResponseRequesterMono::block)
                .hasMessage(String.format(INVALID_PAYLOAD_ERROR_MESSAGE, FRAME_LENGTH_MASK))
                .isInstanceOf(IllegalArgumentException.class));
  }

  /**
   * Ensures that error check happens exactly before frame sent. This cases ensures that in case no
   * lease / other external errors appeared, the local subscriber received the same one. No frames
   * should be sent
   */
  @ParameterizedTest
  @MethodSource("shouldErrorIfNoAvailabilitySource")
  public void shouldErrorIfNoAvailability(Consumer<RequestResponseRequesterMono> monoConsumer) {
    final TestRequesterResponderSupport activeStreams =
        TestRequesterResponderSupport.client(new RuntimeException("test"));
    final LeaksTrackingByteBufAllocator allocator = activeStreams.getAllocator();
    final Payload payload = genericPayload(allocator);

    final RequestResponseRequesterMono requestResponseRequesterMono =
        new RequestResponseRequesterMono(payload, activeStreams);
    final StateAssert<RequestResponseRequesterMono> stateAssert =
        StateAssert.assertThat(requestResponseRequesterMono);

    activeStreams.assertNoActiveStreams();
    stateAssert.isUnsubscribed();

    monoConsumer.accept(requestResponseRequesterMono);

    Assertions.assertThat(payload.refCnt()).isZero();

    activeStreams.assertNoActiveStreams();
    stateAssert.isTerminated();
    allocator.assertHasNoLeaks();
  }

  static Stream<Consumer<RequestResponseRequesterMono>> shouldErrorIfNoAvailabilitySource() {
    return Stream.of(
        (s) ->
            StepVerifier.create(s, 0)
                .expectSubscription()
                .then(() -> StateAssert.assertThat(s).hasSubscribedFlagOnly())
                .thenRequest(1)
                .consumeErrorWith(
                    t ->
                        Assertions.assertThat(t)
                            .hasMessage("test")
                            .isInstanceOf(RuntimeException.class))
                .verify(),
        requestResponseRequesterMono ->
            Assertions.assertThatThrownBy(requestResponseRequesterMono::block)
                .hasMessage("test")
                .isInstanceOf(RuntimeException.class));
  }

  @Test
  public void checkName() {
    final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
    final LeaksTrackingByteBufAllocator allocator = activeStreams.getAllocator();
    final Payload payload = genericPayload(allocator);

    final RequestResponseRequesterMono requestResponseRequesterMono =
        new RequestResponseRequesterMono(payload, activeStreams);

    Assertions.assertThat(Scannable.from(requestResponseRequesterMono).name())
        .isEqualTo("source(RequestResponseMono)");
    requestResponseRequesterMono.cancel();
    allocator.assertHasNoLeaks();
  }
}
