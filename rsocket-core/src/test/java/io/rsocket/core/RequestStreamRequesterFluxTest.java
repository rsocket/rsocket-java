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
import static io.rsocket.core.FragmentationUtils.FRAME_OFFSET_WITH_METADATA_AND_INITIAL_REQUEST_N;
import static io.rsocket.core.PayloadValidationUtils.INVALID_PAYLOAD_ERROR_MESSAGE;
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
import io.rsocket.internal.subscriber.AssertSubscriber;
import io.rsocket.test.util.TestDuplexConnection;
import io.rsocket.util.ByteBufPayload;
import io.rsocket.util.EmptyPayload;
import java.time.Duration;
import java.util.ArrayList;
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

public class RequestStreamRequesterFluxTest {

  @BeforeAll
  public static void setUp() {
    StepVerifier.setDefaultTimeout(Duration.ofSeconds(2));
  }

  /*
   * +-------------------------------+
   * |      General Test Cases       |
   * +-------------------------------+
   */

  /**
   * State Machine check. Ensure migration from
   *
   * <pre>
   * UNSUBSCRIBED -> SUBSCRIBED
   * SUBSCRIBED -> REQUESTED(1) -> REQUESTED(0)
   * REQUESTED(0) -> REQUESTED(1) -> REQUESTED(0)
   * REQUESTED(0) -> REQUESTED(MAX)
   * REQUESTED(MAX) -> REQUESTED(MAX) && REASSEMBLY (extra flag enabled which indicates
   * reassembly)
   * REQUESTED(MAX) && REASSEMBLY -> TERMINATED
   * </pre>
   */
  @Test
  public void requestNFrameShouldBeSentOnSubscriptionAndThenSeparately() {
    final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
    final LeaksTrackingByteBufAllocator allocator = activeStreams.getAllocator();
    final TestDuplexConnection sender = activeStreams.getDuplexConnection();
    final Payload payload = TestRequesterResponderSupport.genericPayload(allocator);

    final RequestStreamRequesterFlux requestStreamRequesterFlux =
        new RequestStreamRequesterFlux(payload, activeStreams);
    final StateAssert<RequestStreamRequesterFlux> stateAssert =
        StateAssert.assertThat(requestStreamRequesterFlux);

    // state machine check

    stateAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    final AssertSubscriber<Payload> assertSubscriber =
        requestStreamRequesterFlux.subscribeWith(AssertSubscriber.create(0));
    Assertions.assertThat(payload.refCnt()).isOne();
    activeStreams.assertNoActiveStreams();
    // state machine check
    stateAssert.hasSubscribedFlagOnly();

    assertSubscriber.request(1);

    Assertions.assertThat(payload.refCnt()).isZero();
    activeStreams.assertHasStream(1, requestStreamRequesterFlux);

    // state machine check
    stateAssert.hasSubscribedFlag().hasRequestN(1).hasFirstFrameSentFlag();

    final ByteBuf frame = sender.awaitFrame();
    FrameAssert.assertThat(frame)
        .isNotNull()
        .hasPayloadSize(
            "testData".getBytes(CharsetUtil.UTF_8).length
                + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
        .hasMetadata("testMetadata")
        .hasData("testData")
        .hasNoFragmentsFollow()
        .hasRequestN(1)
        .typeOf(FrameType.REQUEST_STREAM)
        .hasClientSideStreamId()
        .hasStreamId(1)
        .hasNoLeaks();

    Assertions.assertThat(sender.isEmpty()).isTrue();

    assertSubscriber.request(1);
    final ByteBuf requestNFrame = sender.awaitFrame();
    FrameAssert.assertThat(requestNFrame)
        .isNotNull()
        .hasRequestN(1)
        .typeOf(FrameType.REQUEST_N)
        .hasClientSideStreamId()
        .hasStreamId(1)
        .hasNoLeaks();

    Assertions.assertThat(sender.isEmpty()).isTrue();

    // state machine check. Request N Frame should sent so request field should be 0
    // state machine check
    stateAssert.hasSubscribedFlag().hasRequestN(2).hasFirstFrameSentFlag();

    assertSubscriber.request(Long.MAX_VALUE);
    final ByteBuf requestMaxNFrame = sender.awaitFrame();
    FrameAssert.assertThat(requestMaxNFrame)
        .isNotNull()
        .hasRequestN(Integer.MAX_VALUE)
        .typeOf(FrameType.REQUEST_N)
        .hasClientSideStreamId()
        .hasStreamId(1)
        .hasNoLeaks();

    Assertions.assertThat(sender.isEmpty()).isTrue();

    // state machine check
    stateAssert.hasSubscribedFlag().hasRequestN(Integer.MAX_VALUE).hasFirstFrameSentFlag();

    assertSubscriber.request(6);
    Assertions.assertThat(sender.isEmpty()).isTrue();

    // state machine check
    stateAssert.hasSubscribedFlag().hasRequestN(Integer.MAX_VALUE).hasFirstFrameSentFlag();

    int mtu = ThreadLocalRandom.current().nextInt(64, 256);
    Payload randomPayload = TestRequesterResponderSupport.randomPayload(allocator);
    ArrayList<ByteBuf> fragments =
        TestRequesterResponderSupport.prepareFragments(allocator, mtu, randomPayload);
    ByteBuf firstFragment = fragments.remove(0);
    requestStreamRequesterFlux.handleNext(firstFragment, true, false);
    firstFragment.release();

    // state machine check
    stateAssert
        .hasSubscribedFlag()
        .hasRequestN(Integer.MAX_VALUE)
        .hasFirstFrameSentFlag()
        .hasReassemblingFlag();

    for (int i = 0; i < fragments.size(); i++) {
      boolean hasFollowing = i != fragments.size() - 1;
      ByteBuf followingFragment = fragments.get(i);

      requestStreamRequesterFlux.handleNext(followingFragment, hasFollowing, false);
      followingFragment.release();
    }

    // state machine check
    stateAssert
        .hasSubscribedFlag()
        .hasRequestN(Integer.MAX_VALUE)
        .hasFirstFrameSentFlag()
        .hasNoReassemblingFlag();

    Payload finalRandomPayload = TestRequesterResponderSupport.randomPayload(allocator);
    requestStreamRequesterFlux.handlePayload(finalRandomPayload);
    requestStreamRequesterFlux.handleComplete();

    assertSubscriber
        .assertValuesWith(
            p -> PayloadAssert.assertThat(p).isEqualTo(randomPayload).hasNoLeaks(),
            p -> PayloadAssert.assertThat(p).isEqualTo(finalRandomPayload).hasNoLeaks())
        .assertComplete();

    PayloadAssert.assertThat(randomPayload).hasNoLeaks();

    Assertions.assertThat(payload.refCnt()).isZero();
    activeStreams.assertNoActiveStreams();

    Assertions.assertThat(sender.isEmpty()).isTrue();

    // state machine check
    stateAssert.isTerminated();
    allocator.assertHasNoLeaks();
  }

  /**
   * State Machine check. Ensure migration from
   *
   * <pre>
   * UNSUBSCRIBED -> SUBSCRIBED
   * SUBSCRIBED -> REQUESTED(MAX)
   * REQUESTED(MAX) -> TERMINATED
   * </pre>
   */
  @Test
  public void requestNFrameShouldBeSentExactlyOnceIfItIsMaxAllowed() {
    final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
    final LeaksTrackingByteBufAllocator allocator = activeStreams.getAllocator();
    final TestDuplexConnection sender = activeStreams.getDuplexConnection();
    final Payload payload = TestRequesterResponderSupport.genericPayload(allocator);

    final RequestStreamRequesterFlux requestStreamRequesterFlux =
        new RequestStreamRequesterFlux(payload, activeStreams);
    final StateAssert<RequestStreamRequesterFlux> stateAssert =
        StateAssert.assertThat(requestStreamRequesterFlux);

    // state machine check

    stateAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    final AssertSubscriber<Payload> assertSubscriber =
        requestStreamRequesterFlux.subscribeWith(AssertSubscriber.create(0));
    Assertions.assertThat(payload.refCnt()).isOne();
    activeStreams.assertNoActiveStreams();

    // state machine check
    stateAssert.hasSubscribedFlagOnly();

    assertSubscriber.request(Long.MAX_VALUE / 2 + 1);

    // state machine check

    stateAssert.hasSubscribedFlag().hasRequestN(Integer.MAX_VALUE).hasFirstFrameSentFlag();

    Assertions.assertThat(payload.refCnt()).isZero();
    activeStreams.assertHasStream(1, requestStreamRequesterFlux);

    final ByteBuf frame = sender.awaitFrame();
    FrameAssert.assertThat(frame)
        .isNotNull()
        .hasPayloadSize(
            "testData".getBytes(CharsetUtil.UTF_8).length
                + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
        .hasMetadata("testMetadata")
        .hasData("testData")
        .hasNoFragmentsFollow()
        .hasRequestN(Integer.MAX_VALUE)
        .typeOf(FrameType.REQUEST_STREAM)
        .hasClientSideStreamId()
        .hasStreamId(1)
        .hasNoLeaks();

    Assertions.assertThat(sender.isEmpty()).isTrue();

    assertSubscriber.request(1);
    Assertions.assertThat(sender.isEmpty()).isTrue();

    // state machine check

    stateAssert.hasSubscribedFlag().hasRequestN(Integer.MAX_VALUE).hasFirstFrameSentFlag();

    requestStreamRequesterFlux.handlePayload(EmptyPayload.INSTANCE);
    requestStreamRequesterFlux.handleComplete();

    assertSubscriber.assertValues(EmptyPayload.INSTANCE).assertComplete();

    Assertions.assertThat(payload.refCnt()).isZero();
    activeStreams.assertNoActiveStreams();
    // state machine check
    stateAssert.isTerminated();
    Assertions.assertThat(sender.isEmpty()).isTrue();
    allocator.assertHasNoLeaks();
  }

  /**
   * State Machine check. Ensure migration from
   *
   * <pre>
   * UNSUBSCRIBED -> SUBSCRIBED
   * SUBSCRIBED -> REQUESTED(1) -> REQUESTED(0)
   * </pre>
   *
   * And then for the following cases:
   *
   * <pre>
   * [0]: REQUESTED(0) -> REQUESTED(MAX) (with onNext and few extra request(1) which should not
   * affect state anyhow and should not sent any extra frames)
   *      REQUESTED(MAX) -> TERMINATED
   *
   * [1]: REQUESTED(0) -> REQUESTED(MAX) (with onComplete rightaway)
   *      REQUESTED(MAX) -> TERMINATED
   *
   * [2]: REQUESTED(0) -> REQUESTED(MAX) (with onError rightaway)
   *      REQUESTED(MAX) -> TERMINATED
   *
   * [3]: REQUESTED(0) -> REASSEMBLY
   *      REASSEMBLY -> REASSEMBLY && REQUESTED(MAX)
   *      REASSEMBLY && REQUESTED(MAX) -> REQUESTED(MAX)
   *      REQUESTED(MAX) -> TERMINATED
   *
   * [4]: REQUESTED(0) -> REQUESTED(MAX)
   *      REQUESTED(MAX) -> REASSEMBLY && REQUESTED(MAX)
   *      REASSEMBLY && REQUESTED(MAX) -> TERMINATED (because of cancel() invocation)
   * </pre>
   */
  @ParameterizedTest
  @MethodSource("frameShouldBeSentOnFirstRequestResponses")
  public void frameShouldBeSentOnFirstRequest(
      BiFunction<RequestStreamRequesterFlux, StepVerifier.Step<Payload>, StepVerifier>
          transformer) {
    final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
    final LeaksTrackingByteBufAllocator allocator = activeStreams.getAllocator();
    final TestDuplexConnection sender = activeStreams.getDuplexConnection();
    final Payload payload = TestRequesterResponderSupport.genericPayload(allocator);

    final RequestStreamRequesterFlux requestStreamRequesterFlux =
        new RequestStreamRequesterFlux(payload, activeStreams);
    final StateAssert<RequestStreamRequesterFlux> stateAssert =
        StateAssert.assertThat(requestStreamRequesterFlux);

    // state machine check

    stateAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    transformer
        .apply(
            requestStreamRequesterFlux,
            StepVerifier.create(requestStreamRequesterFlux, 0)
                .expectSubscription()
                .then(
                    () ->
                        // state machine check
                        stateAssert.hasSubscribedFlagOnly())
                .then(() -> Assertions.assertThat(payload.refCnt()).isOne())
                .then(() -> activeStreams.assertNoActiveStreams())
                .thenRequest(1)
                .then(
                    () ->
                        // state machine check
                        stateAssert.hasSubscribedFlag().hasRequestN(1).hasFirstFrameSentFlag())
                .then(() -> Assertions.assertThat(payload.refCnt()).isZero())
                .then(() -> activeStreams.assertHasStream(1, requestStreamRequesterFlux)))
        .verify();

    Assertions.assertThat(payload.refCnt()).isZero();
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
        .hasRequestN(1)
        .typeOf(FrameType.REQUEST_STREAM)
        .hasClientSideStreamId()
        .hasStreamId(1)
        .hasNoLeaks();

    final ByteBuf requestNFrame = sender.awaitFrame();
    FrameAssert.assertThat(requestNFrame)
        .isNotNull()
        .typeOf(FrameType.REQUEST_N)
        .hasRequestN(Integer.MAX_VALUE)
        .hasClientSideStreamId()
        .hasStreamId(1)
        .hasNoLeaks();

    if (!sender.isEmpty()) {
      final ByteBuf cancelFrame = sender.awaitFrame();
      FrameAssert.assertThat(cancelFrame)
          .isNotNull()
          .typeOf(FrameType.CANCEL)
          .hasClientSideStreamId()
          .hasStreamId(1)
          .hasNoLeaks();
    }
    // state machine check
    stateAssert.isTerminated();
    Assertions.assertThat(sender.isEmpty()).isTrue();
    allocator.assertHasNoLeaks();
  }

  static Stream<BiFunction<RequestStreamRequesterFlux, StepVerifier.Step<Payload>, StepVerifier>>
      frameShouldBeSentOnFirstRequestResponses() {
    return Stream.of(
        (rsf, sv) ->
            sv.then(() -> rsf.handlePayload(EmptyPayload.INSTANCE))
                .expectNext(EmptyPayload.INSTANCE)
                .thenRequest(Long.MAX_VALUE)
                .then(
                    () ->
                        // state machine check
                        StateAssert.assertThat(rsf)
                            .hasSubscribedFlag()
                            .hasRequestN(Integer.MAX_VALUE)
                            .hasFirstFrameSentFlag())
                .then(() -> rsf.handlePayload(EmptyPayload.INSTANCE))
                .thenRequest(1L)
                .then(
                    () ->
                        // state machine check
                        StateAssert.assertThat(rsf)
                            .hasSubscribedFlag()
                            .hasRequestN(Integer.MAX_VALUE)
                            .hasFirstFrameSentFlag())
                .expectNext(EmptyPayload.INSTANCE)
                .thenRequest(1L)
                .then(
                    () ->
                        // state machine check
                        StateAssert.assertThat(rsf)
                            .hasSubscribedFlag()
                            .hasRequestN(Integer.MAX_VALUE)
                            .hasFirstFrameSentFlag())
                .then(rsf::handleComplete)
                .thenRequest(1L)
                .then(
                    () ->
                        // state machine check
                        StateAssert.assertThat(rsf).isTerminated())
                .expectComplete(),
        (rsf, sv) ->
            sv.then(() -> rsf.handlePayload(EmptyPayload.INSTANCE))
                .expectNext(EmptyPayload.INSTANCE)
                .thenRequest(Long.MAX_VALUE)
                .then(
                    () ->
                        // state machine check
                        StateAssert.assertThat(rsf)
                            .hasSubscribedFlag()
                            .hasRequestN(Integer.MAX_VALUE)
                            .hasFirstFrameSentFlag())
                .then(rsf::handleComplete)
                .thenRequest(1L)
                .then(
                    () ->
                        // state machine check
                        StateAssert.assertThat(rsf).isTerminated())
                .expectComplete(),
        (rsf, sv) ->
            sv.then(() -> rsf.handlePayload(EmptyPayload.INSTANCE))
                .expectNext(EmptyPayload.INSTANCE)
                .thenRequest(Long.MAX_VALUE)
                .then(
                    () ->
                        // state machine check
                        StateAssert.assertThat(rsf)
                            .hasSubscribedFlag()
                            .hasRequestN(Integer.MAX_VALUE)
                            .hasFirstFrameSentFlag())
                .then(() -> rsf.handleError(new ApplicationErrorException("test")))
                .then(
                    () ->
                        // state machine check
                        StateAssert.assertThat(rsf).isTerminated())
                .thenRequest(1L)
                .thenRequest(1L)
                .expectErrorSatisfies(
                    t ->
                        Assertions.assertThat(t)
                            .hasMessage("test")
                            .isInstanceOf(ApplicationErrorException.class)),
        (rsf, sv) -> {
          final byte[] metadata = new byte[65];
          final byte[] data = new byte[129];
          ThreadLocalRandom.current().nextBytes(metadata);
          ThreadLocalRandom.current().nextBytes(data);

          final Payload payload = ByteBufPayload.create(data, metadata);
          final Payload payload2 = ByteBufPayload.create(data, metadata);

          return sv.then(
                  () -> {
                    final ByteBuf followingFrame =
                        FragmentationUtils.encodeFirstFragment(
                            rsf.allocator,
                            64,
                            FrameType.NEXT,
                            1,
                            payload.hasMetadata(),
                            payload.metadata(),
                            payload.data());
                    rsf.handleNext(followingFrame, true, false);
                    followingFrame.release();
                  })
              .then(
                  () ->
                      // state machine check
                      StateAssert.assertThat(rsf)
                          .hasRequestN(1)
                          .hasSubscribedFlag()
                          .hasFirstFrameSentFlag()
                          .hasReassemblingFlag())
              .then(
                  () -> {
                    final ByteBuf followingFrame =
                        FragmentationUtils.encodeFollowsFragment(
                            rsf.allocator, 64, 1, false, payload.metadata(), payload.data());
                    rsf.handleNext(followingFrame, true, false);
                    followingFrame.release();
                  })
              .then(
                  () ->
                      // state machine check
                      StateAssert.assertThat(rsf)
                          .hasRequestN(1)
                          .hasSubscribedFlag()
                          .hasFirstFrameSentFlag()
                          .hasReassemblingFlag())
              .then(
                  () -> {
                    final ByteBuf followingFrame =
                        FragmentationUtils.encodeFollowsFragment(
                            rsf.allocator, 64, 1, false, payload.metadata(), payload.data());
                    rsf.handleNext(followingFrame, true, false);
                    followingFrame.release();
                  })
              .then(
                  () ->
                      // state machine check
                      StateAssert.assertThat(rsf)
                          .hasRequestN(1)
                          .hasSubscribedFlag()
                          .hasFirstFrameSentFlag()
                          .hasReassemblingFlag())
              .thenRequest(Long.MAX_VALUE)
              .then(
                  () ->
                      // state machine check
                      StateAssert.assertThat(rsf)
                          .hasRequestN(Integer.MAX_VALUE)
                          .hasSubscribedFlag()
                          .hasFirstFrameSentFlag()
                          .hasReassemblingFlag())
              .then(
                  () -> {
                    final ByteBuf followingFrame =
                        FragmentationUtils.encodeFollowsFragment(
                            rsf.allocator, 64, 1, false, payload.metadata(), payload.data());
                    rsf.handleNext(followingFrame, false, false);
                    followingFrame.release();
                  })
              .then(
                  () ->
                      // state machine check
                      StateAssert.assertThat(rsf)
                          .hasRequestN(Integer.MAX_VALUE)
                          .hasSubscribedFlag()
                          .hasFirstFrameSentFlag()
                          .hasNoReassemblingFlag())
              .assertNext(
                  p -> {
                    Assertions.assertThat(p.data()).isEqualTo(Unpooled.wrappedBuffer(data));

                    Assertions.assertThat(p.metadata()).isEqualTo(Unpooled.wrappedBuffer(metadata));
                    Assertions.assertThat(p.release()).isTrue();
                    Assertions.assertThat(p.refCnt()).isZero();
                  })
              .then(payload::release)
              .then(() -> rsf.handlePayload(payload2))
              .thenRequest(1)
              .then(
                  () ->
                      // state machine check
                      StateAssert.assertThat(rsf)
                          .hasRequestN(Integer.MAX_VALUE)
                          .hasSubscribedFlag()
                          .hasFirstFrameSentFlag())
              .assertNext(
                  p -> {
                    Assertions.assertThat(p.data()).isEqualTo(Unpooled.wrappedBuffer(data));

                    Assertions.assertThat(p.metadata()).isEqualTo(Unpooled.wrappedBuffer(metadata));
                    Assertions.assertThat(p.release()).isTrue();
                    Assertions.assertThat(p.refCnt()).isZero();
                  })
              .thenRequest(1)
              .then(
                  () ->
                      // state machine check
                      StateAssert.assertThat(rsf)
                          .hasRequestN(Integer.MAX_VALUE)
                          .hasSubscribedFlag()
                          .hasFirstFrameSentFlag())
              .then(rsf::handleComplete)
              .then(
                  () ->
                      // state machine check
                      StateAssert.assertThat(rsf).isTerminated())
              .expectComplete();
        },
        (rsf, sv) -> {
          final byte[] metadata = new byte[65];
          final byte[] data = new byte[129];
          ThreadLocalRandom.current().nextBytes(metadata);
          ThreadLocalRandom.current().nextBytes(data);

          final Payload payload0 = ByteBufPayload.create(data, metadata);
          final Payload payload = ByteBufPayload.create(data, metadata);

          ByteBuf[] fragments =
              new ByteBuf[] {
                FragmentationUtils.encodeFirstFragment(
                    rsf.allocator,
                    64,
                    FrameType.NEXT,
                    1,
                    payload.hasMetadata(),
                    payload.metadata(),
                    payload.data()),
                FragmentationUtils.encodeFollowsFragment(
                    rsf.allocator, 64, 1, false, payload.metadata(), payload.data()),
                FragmentationUtils.encodeFollowsFragment(
                    rsf.allocator, 64, 1, false, payload.metadata(), payload.data())
              };

          final StepVerifier stepVerifier =
              sv.then(() -> rsf.handlePayload(payload0))
                  .assertNext(p -> PayloadAssert.assertThat(p).isEqualTo(payload0).hasNoLeaks())
                  .thenRequest(Long.MAX_VALUE)
                  .then(
                      () ->
                          // state machine check
                          StateAssert.assertThat(rsf)
                              .hasSubscribedFlag()
                              .hasRequestN(Integer.MAX_VALUE)
                              .hasFirstFrameSentFlag()
                              .hasNoReassemblingFlag())
                  .then(
                      () -> {
                        rsf.handleNext(fragments[0], true, false);
                        fragments[0].release();
                      })
                  .then(
                      () ->
                          // state machine check
                          StateAssert.assertThat(rsf)
                              .hasSubscribedFlag()
                              .hasRequestN(Integer.MAX_VALUE)
                              .hasFirstFrameSentFlag()
                              .hasReassemblingFlag())
                  .then(
                      () -> {
                        rsf.handleNext(fragments[1], true, false);
                        fragments[1].release();
                      })
                  .then(
                      () ->
                          // state machine check
                          StateAssert.assertThat(rsf)
                              .hasSubscribedFlag()
                              .hasRequestN(Integer.MAX_VALUE)
                              .hasFirstFrameSentFlag()
                              .hasReassemblingFlag())
                  .then(
                      () -> {
                        rsf.handleNext(fragments[2], true, false);
                        fragments[2].release();
                      })
                  .then(
                      () ->
                          // state machine check
                          StateAssert.assertThat(rsf)
                              .hasSubscribedFlag()
                              .hasRequestN(Integer.MAX_VALUE)
                              .hasFirstFrameSentFlag()
                              .hasReassemblingFlag())
                  .thenRequest(1)
                  .then(
                      () ->
                          // state machine check
                          StateAssert.assertThat(rsf)
                              .hasSubscribedFlag()
                              .hasRequestN(Integer.MAX_VALUE)
                              .hasFirstFrameSentFlag()
                              .hasReassemblingFlag())
                  .thenRequest(1)
                  .then(
                      () ->
                          // state machine check
                          StateAssert.assertThat(rsf)
                              .hasSubscribedFlag()
                              .hasRequestN(Integer.MAX_VALUE)
                              .hasFirstFrameSentFlag()
                              .hasReassemblingFlag())
                  .then(payload::release)
                  .thenCancel()
                  .verifyLater();

          stepVerifier.verify();
          // state machine check
          StateAssert.assertThat(rsf).isTerminated();

          Assertions.assertThat(fragments).allMatch(bb -> bb.refCnt() == 0);

          return stepVerifier;
        });
  }

  /**
   * State Machine check with fragmentation of the first payload. Ensure migration from
   *
   * <pre>
   * UNSUBSCRIBED -> SUBSCRIBED
   * SUBSCRIBED -> REQUESTED(1) -> REQUESTED(0)
   * </pre>
   *
   * And then for the following cases:
   *
   * <pre>
   * [0]: REQUESTED(0) -> REQUESTED(MAX) (with onNext and few extra request(1) which should not
   * affect state anyhow and should not sent any extra frames)
   *      REQUESTED(MAX) -> TERMINATED
   *
   * [1]: REQUESTED(0) -> REQUESTED(MAX) (with onComplete rightaway)
   *      REQUESTED(MAX) -> TERMINATED
   *
   * [2]: REQUESTED(0) -> REQUESTED(MAX) (with onError rightaway)
   *      REQUESTED(MAX) -> TERMINATED
   *
   * [3]: REQUESTED(0) -> REASSEMBLY
   *      REASSEMBLY -> REASSEMBLY && REQUESTED(MAX)
   *      REASSEMBLY && REQUESTED(MAX) -> REQUESTED(MAX)
   *      REQUESTED(MAX) -> TERMINATED
   *
   * [4]: REQUESTED(0) -> REQUESTED(MAX)
   *      REQUESTED(MAX) -> REASSEMBLY && REQUESTED(MAX)
   *      REASSEMBLY && REQUESTED(MAX) -> TERMINATED (because of cancel() invocation)
   * </pre>
   */
  @ParameterizedTest
  @MethodSource("frameShouldBeSentOnFirstRequestResponses")
  public void frameFragmentsShouldBeSentOnFirstRequest(
      BiFunction<RequestStreamRequesterFlux, StepVerifier.Step<Payload>, StepVerifier>
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

    final RequestStreamRequesterFlux requestStreamRequesterFlux =
        new RequestStreamRequesterFlux(payload, activeStreams);
    final StateAssert<RequestStreamRequesterFlux> stateAssert =
        StateAssert.assertThat(requestStreamRequesterFlux);

    // state machine check
    stateAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    transformer
        .apply(
            requestStreamRequesterFlux,
            StepVerifier.create(requestStreamRequesterFlux, 0)
                .expectSubscription()
                .then(() -> Assertions.assertThat(payload.refCnt()).isOne())
                .then(() -> activeStreams.assertNoActiveStreams())
                .thenRequest(1)
                .then(() -> Assertions.assertThat(payload.refCnt()).isZero())
                .then(() -> activeStreams.assertHasStream(1, requestStreamRequesterFlux)))
        .verify();

    // should not add anything to map
    activeStreams.assertNoActiveStreams();

    Assertions.assertThat(payload.refCnt()).isZero();

    final ByteBuf frameFragment1 = sender.awaitFrame();
    FrameAssert.assertThat(frameFragment1)
        .isNotNull()
        .hasPayloadSize(64 - FRAME_OFFSET_WITH_METADATA_AND_INITIAL_REQUEST_N)
        // InitialRequestN size
        .hasMetadata(Arrays.copyOf(metadata, 64 - FRAME_OFFSET_WITH_METADATA_AND_INITIAL_REQUEST_N))
        .hasData(Unpooled.EMPTY_BUFFER)
        .hasFragmentsFollow()
        .typeOf(FrameType.REQUEST_STREAM)
        .hasClientSideStreamId()
        .hasStreamId(1)
        .hasNoLeaks();

    final ByteBuf frameFragment2 = sender.awaitFrame();
    FrameAssert.assertThat(frameFragment2)
        .isNotNull()
        .hasPayloadSize(64 - FRAME_OFFSET_WITH_METADATA)
        .hasMetadata(
            Arrays.copyOfRange(metadata, 64 - FRAME_OFFSET_WITH_METADATA_AND_INITIAL_REQUEST_N, 65))
        .hasData(Arrays.copyOf(data, 35))
        .hasFragmentsFollow()
        .typeOf(FrameType.NEXT)
        .hasClientSideStreamId()
        .hasStreamId(1)
        .hasNoLeaks();

    final ByteBuf frameFragment3 = sender.awaitFrame();
    FrameAssert.assertThat(frameFragment3)
        .isNotNull()
        .hasPayloadSize(64 - FRAME_OFFSET)
        .hasNoMetadata()
        .hasData(Arrays.copyOfRange(data, 35, 35 + 55))
        .hasFragmentsFollow()
        .typeOf(FrameType.NEXT)
        .hasClientSideStreamId()
        .hasStreamId(1)
        .hasNoLeaks();

    final ByteBuf frameFragment4 = sender.awaitFrame();
    FrameAssert.assertThat(frameFragment4)
        .isNotNull()
        .hasPayloadSize(39)
        .hasNoMetadata()
        .hasData(Arrays.copyOfRange(data, 90, 129))
        .hasNoFragmentsFollow()
        .typeOf(FrameType.NEXT)
        .hasClientSideStreamId()
        .hasStreamId(1)
        .hasNoLeaks();

    final ByteBuf requestNFrame = sender.awaitFrame();
    FrameAssert.assertThat(requestNFrame)
        .isNotNull()
        .typeOf(FrameType.REQUEST_N)
        .hasRequestN(Integer.MAX_VALUE)
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
    // state machine check
    stateAssert.isTerminated();
    allocator.assertHasNoLeaks();
  }

  /**
   * Case which ensures that if Payload has incorrect refCnt, the flux ends up with an appropriate
   * error
   */
  @ParameterizedTest
  @MethodSource("shouldErrorOnIncorrectRefCntInGivenPayloadSource")
  public void shouldErrorOnIncorrectRefCntInGivenPayload(
      Consumer<RequestStreamRequesterFlux> monoConsumer) {
    final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
    final LeaksTrackingByteBufAllocator allocator = activeStreams.getAllocator();
    final TestDuplexConnection sender = activeStreams.getDuplexConnection();
    final Payload payload = ByteBufPayload.create("");
    payload.release();

    final RequestStreamRequesterFlux requestStreamRequesterFlux =
        new RequestStreamRequesterFlux(payload, activeStreams);
    final StateAssert<RequestStreamRequesterFlux> stateAssert =
        StateAssert.assertThat(requestStreamRequesterFlux);

    // state machine check
    stateAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    monoConsumer.accept(requestStreamRequesterFlux);

    activeStreams.assertNoActiveStreams();
    Assertions.assertThat(sender.isEmpty()).isTrue();
    // state machine check
    stateAssert.isTerminated();
    allocator.assertHasNoLeaks();
  }

  static Stream<Consumer<RequestStreamRequesterFlux>>
      shouldErrorOnIncorrectRefCntInGivenPayloadSource() {
    return Stream.of(
        (s) ->
            StepVerifier.create(s)
                .expectSubscription()
                .expectError(IllegalReferenceCountException.class)
                .verify(),
        requestStreamRequesterFlux ->
            Assertions.assertThatThrownBy(requestStreamRequesterFlux::blockLast)
                .isInstanceOf(IllegalReferenceCountException.class));
  }

  /**
   * Ensures that if Payload is release right after the subscription, the first request will exponse
   * the error immediatelly and no frame will be sent to the remote party
   */
  @Test
  public void shouldErrorOnIncorrectRefCntInGivenPayloadLatePhase() {
    final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
    final LeaksTrackingByteBufAllocator allocator = activeStreams.getAllocator();
    final TestDuplexConnection sender = activeStreams.getDuplexConnection();
    ;
    final Payload payload = ByteBufPayload.create("");

    final RequestStreamRequesterFlux requestStreamRequesterFlux =
        new RequestStreamRequesterFlux(payload, activeStreams);
    final StateAssert<RequestStreamRequesterFlux> stateAssert =
        StateAssert.assertThat(requestStreamRequesterFlux);

    // state machine check
    stateAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    StepVerifier.create(requestStreamRequesterFlux, 0)
        .expectSubscription()
        .then(
            () ->
                // state machine check
                stateAssert.hasSubscribedFlagOnly())
        .then(payload::release)
        .thenRequest(1)
        .then(
            () ->
                // state machine check
                stateAssert.isTerminated())
        .expectError(IllegalReferenceCountException.class)
        .verify();

    activeStreams.assertNoActiveStreams();
    Assertions.assertThat(sender.isEmpty()).isTrue();

    // state machine check
    stateAssert.isTerminated();
    allocator.assertHasNoLeaks();
  }

  /**
   * Ensures that if Payload is release right after the subscription, the first request will expose
   * the error immediately and no frame will be sent to the remote party
   */
  @Test
  public void shouldErrorOnIncorrectRefCntInGivenPayloadLatePhaseWithFragmentation() {
    final int mtu = 64;
    final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client(mtu);
    final LeaksTrackingByteBufAllocator allocator = activeStreams.getAllocator();
    final TestDuplexConnection sender = activeStreams.getDuplexConnection();

    final byte[] metadata = new byte[65];
    final byte[] data = new byte[129];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);

    final Payload payload = ByteBufPayload.create(data, metadata);

    final RequestStreamRequesterFlux requestStreamRequesterFlux =
        new RequestStreamRequesterFlux(payload, activeStreams);
    final StateAssert<RequestStreamRequesterFlux> stateAssert =
        StateAssert.assertThat(requestStreamRequesterFlux);

    // state machine check
    stateAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    StepVerifier.create(requestStreamRequesterFlux, 0)
        .expectSubscription()
        .then(
            () ->
                // state machine check
                stateAssert.hasSubscribedFlagOnly())
        .then(payload::release)
        .thenRequest(1)
        .then(
            () ->
                // state machine check
                stateAssert.isTerminated())
        .expectError(IllegalReferenceCountException.class)
        .verify();

    activeStreams.assertNoActiveStreams();
    Assertions.assertThat(sender.isEmpty()).isTrue();
    // state machine check
    stateAssert.isTerminated();
    allocator.assertHasNoLeaks();
  }

  /**
   * Ensures that if the given payload is exits 16mb size with disabled fragmentation, than the
   * appropriate validation happens and a corresponding error will be propagagted to the subscriber
   */
  @ParameterizedTest
  @MethodSource("shouldErrorIfFragmentExitsAllowanceIfFragmentationDisabledSource")
  public void shouldErrorIfFragmentExitsAllowanceIfFragmentationDisabled(
      Consumer<RequestStreamRequesterFlux> monoConsumer) {
    final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
    final LeaksTrackingByteBufAllocator allocator = activeStreams.getAllocator();
    final TestDuplexConnection sender = activeStreams.getDuplexConnection();

    final byte[] metadata = new byte[FRAME_LENGTH_MASK];
    final byte[] data = new byte[FRAME_LENGTH_MASK];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);

    final Payload payload = ByteBufPayload.create(data, metadata);

    final RequestStreamRequesterFlux requestStreamRequesterFlux =
        new RequestStreamRequesterFlux(payload, activeStreams);

    final StateAssert<RequestStreamRequesterFlux> stateAssert =
        StateAssert.assertThat(requestStreamRequesterFlux);

    // state machine check
    stateAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    monoConsumer.accept(requestStreamRequesterFlux);

    Assertions.assertThat(payload.refCnt()).isZero();

    activeStreams.assertNoActiveStreams();
    Assertions.assertThat(sender.isEmpty()).isTrue();
    // state machine check
    stateAssert.isTerminated();
    allocator.assertHasNoLeaks();
  }

  static Stream<Consumer<RequestStreamRequesterFlux>>
      shouldErrorIfFragmentExitsAllowanceIfFragmentationDisabledSource() {
    return Stream.of(
        (s) ->
            StepVerifier.create(s, 0)
                .expectSubscription()
                .then(
                    () ->
                        // state machine check
                        StateAssert.assertThat(s).isTerminated())
                .consumeErrorWith(
                    t ->
                        Assertions.assertThat(t)
                            .hasMessage(
                                String.format(INVALID_PAYLOAD_ERROR_MESSAGE, FRAME_LENGTH_MASK))
                            .isInstanceOf(IllegalArgumentException.class))
                .verify(),
        requestStreamRequesterFlux ->
            Assertions.assertThatThrownBy(requestStreamRequesterFlux::blockLast)
                .hasMessage(String.format(INVALID_PAYLOAD_ERROR_MESSAGE, FRAME_LENGTH_MASK))
                .isInstanceOf(IllegalArgumentException.class));
  }

  /**
   * Ensures that the interactions check and respect rsocket availability (such as leasing) and
   * propagate an error to the final subscriber. No frame should be sent. Check should happens
   * exactly on the first request.
   */
  @ParameterizedTest
  @MethodSource("shouldErrorIfNoAvailabilitySource")
  public void shouldErrorIfNoAvailability(Consumer<RequestStreamRequesterFlux> monoConsumer) {
    final TestRequesterResponderSupport activeStreams =
        TestRequesterResponderSupport.client(new RuntimeException("test"));
    final LeaksTrackingByteBufAllocator allocator = activeStreams.getAllocator();
    final Payload payload = TestRequesterResponderSupport.genericPayload(allocator);

    final RequestStreamRequesterFlux requestStreamRequesterFlux =
        new RequestStreamRequesterFlux(payload, activeStreams);
    final StateAssert<RequestStreamRequesterFlux> stateAssert =
        StateAssert.assertThat(requestStreamRequesterFlux);

    // state machine check
    stateAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    monoConsumer.accept(requestStreamRequesterFlux);

    Assertions.assertThat(payload.refCnt()).isZero();

    activeStreams.assertNoActiveStreams();
    allocator.assertHasNoLeaks();
  }

  static Stream<Consumer<RequestStreamRequesterFlux>> shouldErrorIfNoAvailabilitySource() {
    return Stream.of(
        (s) ->
            StepVerifier.create(s, 0)
                .expectSubscription()
                .then(
                    () ->
                        // state machine check
                        StateAssert.assertThat(s).hasSubscribedFlagOnly())
                .thenRequest(1)
                .then(
                    () ->
                        // state machine check
                        StateAssert.assertThat(s).isTerminated())
                .consumeErrorWith(
                    t ->
                        Assertions.assertThat(t)
                            .hasMessage("test")
                            .isInstanceOf(RuntimeException.class))
                .verify(),
        requestStreamRequesterFlux ->
            Assertions.assertThatThrownBy(requestStreamRequesterFlux::blockLast)
                .hasMessage("test")
                .isInstanceOf(RuntimeException.class));
  }

  @Test
  public void checkName() {
    final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
    final LeaksTrackingByteBufAllocator allocator = activeStreams.getAllocator();
    final Payload payload = TestRequesterResponderSupport.genericPayload(allocator);

    final RequestStreamRequesterFlux requestStreamRequesterFlux =
        new RequestStreamRequesterFlux(payload, activeStreams);

    Assertions.assertThat(Scannable.from(requestStreamRequesterFlux).name())
        .isEqualTo("source(RequestStreamFlux)");
    requestStreamRequesterFlux.cancel();
    allocator.assertHasNoLeaks();
  }
}
