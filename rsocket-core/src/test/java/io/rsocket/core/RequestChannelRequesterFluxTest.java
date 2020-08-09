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
import io.rsocket.util.DefaultPayload;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Signal;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.util.RaceTestUtils;

public class RequestChannelRequesterFluxTest {

  @BeforeAll
  public static void setUp() {
    StepVerifier.setDefaultTimeout(Duration.ofSeconds(2));
  }

  /*
   * +-------------------------------+
   * |      General Test Cases       |
   * +-------------------------------+
   */
  @ParameterizedTest
  @ValueSource(strings = {"inbound", "outbound"})
  public void requestNFrameShouldBeSentOnSubscriptionAndThenSeparately(String completionCase) {
    final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
    final LeaksTrackingByteBufAllocator allocator = activeStreams.getAllocator();
    final TestDuplexConnection sender = activeStreams.getDuplexConnection();
    final Payload payload = TestRequesterResponderSupport.genericPayload(allocator);
    final TestPublisher<Payload> publisher = TestPublisher.create();

    final RequestChannelRequesterFlux requestChannelRequesterFlux =
        new RequestChannelRequesterFlux(publisher, activeStreams);
    final StateAssert<RequestChannelRequesterFlux> stateAssert =
        StateAssert.assertThat(requestChannelRequesterFlux);

    // state machine check

    stateAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    final AssertSubscriber<Payload> assertSubscriber =
        requestChannelRequesterFlux.subscribeWith(AssertSubscriber.create(0));
    Assertions.assertThat(payload.refCnt()).isOne();
    activeStreams.assertNoActiveStreams();
    // state machine check
    stateAssert.hasSubscribedFlagOnly();

    assertSubscriber.request(10);

    Assertions.assertThat(payload.refCnt()).isOne();
    activeStreams.assertNoActiveStreams();

    stateAssert.hasSubscribedFlag().hasRequestN(10).hasNoFirstFrameSentFlag();

    publisher.assertMaxRequested(1).next(payload);

    Assertions.assertThat(payload.refCnt()).isZero();

    activeStreams.assertHasStream(1, requestChannelRequesterFlux);

    // state machine check
    stateAssert.hasSubscribedFlag().hasRequestN(10).hasFirstFrameSentFlag();

    final ByteBuf frame = sender.awaitFrame();
    FrameAssert.assertThat(frame)
        .isNotNull()
        .hasPayloadSize(
            "testData".getBytes(CharsetUtil.UTF_8).length
                + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
        .hasMetadata("testMetadata")
        .hasData("testData")
        .hasNoFragmentsFollow()
        .hasRequestN(10)
        .typeOf(FrameType.REQUEST_CHANNEL)
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
    stateAssert.hasSubscribedFlag().hasRequestN(11).hasFirstFrameSentFlag();

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

    Payload nextPayload = TestRequesterResponderSupport.genericPayload(allocator);
    requestChannelRequesterFlux.handlePayload(nextPayload);

    int mtu = ThreadLocalRandom.current().nextInt(64, 256);
    Payload randomPayload = TestRequesterResponderSupport.randomPayload(allocator);
    ArrayList<ByteBuf> fragments =
        TestRequesterResponderSupport.prepareFragments(allocator, mtu, randomPayload);

    ByteBuf firstFragment = fragments.remove(0);
    requestChannelRequesterFlux.handleNext(firstFragment, true, false);
    firstFragment.release();

    // state machine check
    stateAssert
        .hasSubscribedFlag()
        .hasRequestN(Integer.MAX_VALUE)
        .hasFirstFrameSentFlag()
        .hasReassemblingFlag();

    for (int i = 0; i < fragments.size(); i++) {
      boolean hasFollows = i != fragments.size() - 1;
      ByteBuf followingFragment = fragments.get(i);

      requestChannelRequesterFlux.handleNext(followingFragment, hasFollows, false);
      followingFragment.release();
    }

    // state machine check
    stateAssert
        .hasSubscribedFlag()
        .hasRequestN(Integer.MAX_VALUE)
        .hasFirstFrameSentFlag()
        .hasNoReassemblingFlag();

    if (completionCase.equals("inbound")) {
      requestChannelRequesterFlux.handleComplete();
      assertSubscriber
          .assertValuesWith(
              p -> PayloadAssert.assertThat(p).isEqualTo(nextPayload).hasNoLeaks(),
              p -> {
                PayloadAssert.assertThat(p).isEqualTo(randomPayload).hasNoLeaks();
                randomPayload.release();
              })
          .assertComplete();

      // state machine check
      stateAssert
          .hasSubscribedFlag()
          .hasRequestN(Integer.MAX_VALUE)
          .hasFirstFrameSentFlag()
          .hasNoReassemblingFlag()
          .hasInboundTerminated();

      publisher.complete();
      FrameAssert.assertThat(sender.awaitFrame()).typeOf(FrameType.COMPLETE).hasNoLeaks();
    } else if (completionCase.equals("outbound")) {
      publisher.complete();
      FrameAssert.assertThat(sender.awaitFrame()).typeOf(FrameType.COMPLETE).hasNoLeaks();

      // state machine check
      stateAssert
          .hasSubscribedFlag()
          .hasRequestN(Integer.MAX_VALUE)
          .hasFirstFrameSentFlag()
          .hasNoReassemblingFlag()
          .hasOutboundTerminated();

      requestChannelRequesterFlux.handleComplete();
      assertSubscriber
          .assertValuesWith(
              p -> PayloadAssert.assertThat(p).isEqualTo(nextPayload).hasNoLeaks(),
              p -> {
                PayloadAssert.assertThat(p).isEqualTo(randomPayload).hasNoLeaks();
                randomPayload.release();
              })
          .assertComplete();
    }

    stateAssert.isTerminated();
    activeStreams.assertNoActiveStreams();

    Assertions.assertThat(sender.isEmpty()).isTrue();
    allocator.assertHasNoLeaks();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void streamShouldErrorWithoutInitializingRemoteStreamIfSourceIsEmpty(boolean doRequest) {
    final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
    final LeaksTrackingByteBufAllocator allocator = activeStreams.getAllocator();
    final TestDuplexConnection sender = activeStreams.getDuplexConnection();
    final TestPublisher<Payload> publisher = TestPublisher.create();

    final RequestChannelRequesterFlux requestChannelRequesterFlux =
        new RequestChannelRequesterFlux(publisher, activeStreams);
    final StateAssert<RequestChannelRequesterFlux> stateAssert =
        StateAssert.assertThat(requestChannelRequesterFlux);

    // state machine check

    stateAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    final AssertSubscriber<Payload> assertSubscriber =
        requestChannelRequesterFlux.subscribeWith(AssertSubscriber.create(0));
    activeStreams.assertNoActiveStreams();

    // state machine check
    stateAssert.hasSubscribedFlagOnly();

    if (doRequest) {
      assertSubscriber.request(Integer.MAX_VALUE);
      stateAssert.hasSubscribedFlag().hasRequestN(Integer.MAX_VALUE).hasNoFirstFrameSentFlag();
      activeStreams.assertNoActiveStreams();
    }

    publisher.complete();
    Assertions.assertThat(sender.isEmpty()).isTrue();

    activeStreams.assertNoActiveStreams();
    // state machine check
    stateAssert.isTerminated();
    assertSubscriber
        .assertTerminated()
        .assertError(CancellationException.class)
        .assertErrorMessage("Empty Source");
    allocator.assertHasNoLeaks();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void streamShouldPropagateErrorWithoutInitializingRemoteStreamIfTheFirstSignalIsError(
      boolean doRequest) {
    final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
    final LeaksTrackingByteBufAllocator allocator = activeStreams.getAllocator();
    final TestDuplexConnection sender = activeStreams.getDuplexConnection();
    final TestPublisher<Payload> publisher = TestPublisher.create();

    final RequestChannelRequesterFlux requestChannelRequesterFlux =
        new RequestChannelRequesterFlux(publisher, activeStreams);
    final StateAssert<RequestChannelRequesterFlux> stateAssert =
        StateAssert.assertThat(requestChannelRequesterFlux);

    // state machine check

    stateAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    final AssertSubscriber<Payload> assertSubscriber =
        requestChannelRequesterFlux.subscribeWith(AssertSubscriber.create(0));
    activeStreams.assertNoActiveStreams();

    // state machine check
    stateAssert.hasSubscribedFlagOnly();

    if (doRequest) {
      assertSubscriber.request(Integer.MAX_VALUE);
      stateAssert.hasSubscribedFlag().hasRequestN(Integer.MAX_VALUE).hasNoFirstFrameSentFlag();
      activeStreams.assertNoActiveStreams();
    }

    publisher.error(new RuntimeException("test"));
    Assertions.assertThat(sender.isEmpty()).isTrue();

    activeStreams.assertNoActiveStreams();
    // state machine check
    stateAssert.isTerminated();
    assertSubscriber
        .assertTerminated()
        .assertError(RuntimeException.class)
        .assertErrorMessage("test");
    allocator.assertHasNoLeaks();
  }

  @ParameterizedTest
  @ValueSource(strings = {"inbound", "outbound"})
  public void streamShouldBeInHalfClosedStateOnTheInboundCancellation(String terminationMode) {
    final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
    final LeaksTrackingByteBufAllocator allocator = activeStreams.getAllocator();
    final TestDuplexConnection sender = activeStreams.getDuplexConnection();
    final TestPublisher<Payload> publisher = TestPublisher.create();

    final RequestChannelRequesterFlux requestChannelRequesterFlux =
        new RequestChannelRequesterFlux(publisher, activeStreams);
    final StateAssert<RequestChannelRequesterFlux> stateAssert =
        StateAssert.assertThat(requestChannelRequesterFlux);

    // state machine check

    stateAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    final AssertSubscriber<Payload> assertSubscriber =
        requestChannelRequesterFlux.subscribeWith(AssertSubscriber.create(0));
    activeStreams.assertNoActiveStreams();

    // state machine check
    stateAssert.hasSubscribedFlagOnly();

    assertSubscriber.request(Integer.MAX_VALUE);
    stateAssert.hasSubscribedFlag().hasRequestN(Integer.MAX_VALUE).hasNoFirstFrameSentFlag();
    activeStreams.assertNoActiveStreams();

    Payload payload1 = TestRequesterResponderSupport.randomPayload(allocator);
    Payload payload2 = TestRequesterResponderSupport.randomPayload(allocator);
    Payload payload3 = TestRequesterResponderSupport.randomPayload(allocator);

    publisher.next(payload1.retain());

    FrameAssert.assertThat(sender.awaitFrame())
        .typeOf(FrameType.REQUEST_CHANNEL)
        .hasPayload(payload1)
        .hasRequestN(Integer.MAX_VALUE)
        .hasNoLeaks();
    payload1.release();

    stateAssert.hasSubscribedFlag().hasRequestN(Integer.MAX_VALUE).hasFirstFrameSentFlag();
    activeStreams.assertHasStream(1, requestChannelRequesterFlux);

    publisher.assertMaxRequested(1);

    requestChannelRequesterFlux.handleRequestN(10);
    publisher.assertMaxRequested(10);

    requestChannelRequesterFlux.handleRequestN(Long.MAX_VALUE);
    publisher.assertMaxRequested(Long.MAX_VALUE);

    publisher.next(payload2.retain(), payload3.retain());

    FrameAssert.assertThat(sender.awaitFrame())
        .typeOf(FrameType.NEXT)
        .hasPayload(payload2)
        .hasNoLeaks();
    payload2.release();

    FrameAssert.assertThat(sender.awaitFrame())
        .typeOf(FrameType.NEXT)
        .hasPayload(payload3)
        .hasNoLeaks();
    payload3.release();

    if (terminationMode.equals("outbound")) {
      requestChannelRequesterFlux.handleCancel();

      stateAssert
          .hasSubscribedFlag()
          .hasRequestN(Integer.MAX_VALUE)
          .hasFirstFrameSentFlag()
          .hasOutboundTerminated();

      activeStreams.assertHasStream(1, requestChannelRequesterFlux);

      requestChannelRequesterFlux.handleComplete();
    } else if (terminationMode.equals("inbound")) {
      requestChannelRequesterFlux.handleComplete();

      stateAssert
          .hasSubscribedFlag()
          .hasRequestN(Integer.MAX_VALUE)
          .hasFirstFrameSentFlag()
          .hasInboundTerminated();

      activeStreams.assertHasStream(1, requestChannelRequesterFlux);

      requestChannelRequesterFlux.handleCancel();
    }

    activeStreams.assertNoActiveStreams();
    // state machine check
    stateAssert.isTerminated();
  }

  @ParameterizedTest
  @ValueSource(strings = {"inbound", "outbound"})
  public void errorShouldTerminateExecution(String terminationMode) {
    final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
    final LeaksTrackingByteBufAllocator allocator = activeStreams.getAllocator();
    final TestDuplexConnection sender = activeStreams.getDuplexConnection();
    final TestPublisher<Payload> publisher = TestPublisher.create();

    final RequestChannelRequesterFlux requestChannelRequesterFlux =
        new RequestChannelRequesterFlux(publisher, activeStreams);
    final StateAssert<RequestChannelRequesterFlux> stateAssert =
        StateAssert.assertThat(requestChannelRequesterFlux);

    // state machine check

    stateAssert.isUnsubscribed();
    activeStreams.assertNoActiveStreams();

    final AssertSubscriber<Payload> assertSubscriber =
        requestChannelRequesterFlux.subscribeWith(AssertSubscriber.create(0));
    activeStreams.assertNoActiveStreams();

    // state machine check
    stateAssert.hasSubscribedFlagOnly();

    assertSubscriber.request(Integer.MAX_VALUE);
    stateAssert.hasSubscribedFlag().hasRequestN(Integer.MAX_VALUE).hasNoFirstFrameSentFlag();
    activeStreams.assertNoActiveStreams();

    Payload payload1 = TestRequesterResponderSupport.randomPayload(allocator);
    Payload payload2 = TestRequesterResponderSupport.randomPayload(allocator);
    Payload payload3 = TestRequesterResponderSupport.randomPayload(allocator);

    publisher.next(payload1.retain());

    FrameAssert.assertThat(sender.awaitFrame())
        .typeOf(FrameType.REQUEST_CHANNEL)
        .hasPayload(payload1)
        .hasRequestN(Integer.MAX_VALUE)
        .hasNoLeaks();
    payload1.release();

    stateAssert.hasSubscribedFlag().hasRequestN(Integer.MAX_VALUE).hasFirstFrameSentFlag();
    activeStreams.assertHasStream(1, requestChannelRequesterFlux);

    publisher.assertMaxRequested(1);

    requestChannelRequesterFlux.handleRequestN(10);
    publisher.assertMaxRequested(10);

    requestChannelRequesterFlux.handleRequestN(Long.MAX_VALUE);
    publisher.assertMaxRequested(Long.MAX_VALUE);

    publisher.next(payload2.retain(), payload3.retain());

    FrameAssert.assertThat(sender.awaitFrame())
        .typeOf(FrameType.NEXT)
        .hasPayload(payload2)
        .hasNoLeaks();
    payload2.release();

    FrameAssert.assertThat(sender.awaitFrame())
        .typeOf(FrameType.NEXT)
        .hasPayload(payload3)
        .hasNoLeaks();
    payload3.release();

    if (terminationMode.equals("outbound")) {
      publisher.error(new ApplicationErrorException("test"));
      FrameAssert.assertThat(sender.awaitFrame())
          .typeOf(FrameType.ERROR)
          .hasData("test")
          .hasNoLeaks();
    } else if (terminationMode.equals("inbound")) {
      requestChannelRequesterFlux.handleError(new ApplicationErrorException("test"));
      publisher.assertWasCancelled();
    }

    activeStreams.assertNoActiveStreams();
    // state machine check
    stateAssert.isTerminated();
  }

  /*
   * +--------------------------------+
   * |       Racing Test Cases        |
   * +--------------------------------+
   */

  static Stream<Arguments> cases() {
    return Stream.of(
        Arguments.arguments("complete", "sizeError"),
        Arguments.arguments("complete", "refCntError"),
        Arguments.arguments("complete", "onError"),
        Arguments.arguments("error", "sizeError"),
        Arguments.arguments("error", "refCntError"),
        Arguments.arguments("error", "onError"),
        Arguments.arguments("cancel", "sizeError"),
        Arguments.arguments("cancel", "refCntError"),
        Arguments.arguments("cancel", "onError"));
  }

  @ParameterizedTest
  @MethodSource("cases")
  public void shouldHaveEventsDeliveredSeriallyWhenOutboundErrorRacingWithInboundSignals(
      String inboundTerminationMode, String outboundTerminationMode) {
    final RuntimeException outboundException = new RuntimeException("outboundException");
    final ApplicationErrorException inboundException =
        new ApplicationErrorException("inboundException");

    final ArrayList<Throwable> droppedErrors = new ArrayList<>();
    final Payload oversizePayload =
        DefaultPayload.create(new byte[FRAME_LENGTH_MASK], new byte[FRAME_LENGTH_MASK]);

    Hooks.onErrorDropped(droppedErrors::add);
    try {
      for (int i = 0; i < 10000; i++) {
        final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
        final LeaksTrackingByteBufAllocator allocator = activeStreams.getAllocator();
        final TestDuplexConnection sender = activeStreams.getDuplexConnection();
        final TestPublisher<Payload> publisher =
            TestPublisher.createNoncompliant(TestPublisher.Violation.DEFER_CANCELLATION);

        final RequestChannelRequesterFlux requestChannelRequesterFlux =
            new RequestChannelRequesterFlux(publisher, activeStreams);
        final StateAssert<RequestChannelRequesterFlux> stateAssert =
            StateAssert.assertThat(requestChannelRequesterFlux);

        stateAssert.isUnsubscribed();
        activeStreams.assertNoActiveStreams();

        final AssertSubscriber<Signal<Payload>> assertSubscriber =
            requestChannelRequesterFlux.materialize().subscribeWith(AssertSubscriber.create(0));
        activeStreams.assertNoActiveStreams();

        // state machine check
        stateAssert.hasSubscribedFlagOnly();

        assertSubscriber.request(Integer.MAX_VALUE);
        stateAssert.hasSubscribedFlag().hasRequestN(Integer.MAX_VALUE).hasNoFirstFrameSentFlag();
        activeStreams.assertNoActiveStreams();

        Payload requestPayload = TestRequesterResponderSupport.randomPayload(allocator);
        publisher.next(requestPayload);

        stateAssert.hasSubscribedFlag().hasRequestN(Integer.MAX_VALUE).hasFirstFrameSentFlag();
        activeStreams.assertHasStream(1, requestChannelRequesterFlux);
        FrameAssert.assertThat(sender.awaitFrame())
            .typeOf(FrameType.REQUEST_CHANNEL)
            .hasRequestN(Integer.MAX_VALUE)
            .hasNoLeaks();

        requestChannelRequesterFlux.handleRequestN(Long.MAX_VALUE);

        Payload responsePayload1 = TestRequesterResponderSupport.randomPayload(allocator);
        Payload responsePayload2 = TestRequesterResponderSupport.randomPayload(allocator);
        Payload responsePayload3 = TestRequesterResponderSupport.randomPayload(allocator);

        Payload releasedPayload = ByteBufPayload.create(Unpooled.EMPTY_BUFFER);
        releasedPayload.release();

        RaceTestUtils.race(
            () -> {
              if (outboundTerminationMode.equals("onError")) {
                publisher.error(outboundException);
              } else if (outboundTerminationMode.equals("refCntError")) {
                publisher.next(releasedPayload);
              } else {
                publisher.next(oversizePayload);
              }
            },
            () -> {
              requestChannelRequesterFlux.handlePayload(responsePayload1);
              requestChannelRequesterFlux.handlePayload(responsePayload2);
              requestChannelRequesterFlux.handlePayload(responsePayload3);

              if (inboundTerminationMode.equals("error")) {
                requestChannelRequesterFlux.handleError(inboundException);
              } else if (inboundTerminationMode.equals("complete")) {
                requestChannelRequesterFlux.handleComplete();
              } else {
                requestChannelRequesterFlux.handleCancel();
              }
            });

        ByteBuf errorFrameOrEmpty = sender.pollFrame();
        if (errorFrameOrEmpty != null) {
          if (outboundTerminationMode.equals("onError")) {
            FrameAssert.assertThat(errorFrameOrEmpty)
                .typeOf(FrameType.ERROR)
                .hasData("outboundException")
                .hasNoLeaks();
          } else {
            FrameAssert.assertThat(errorFrameOrEmpty).typeOf(FrameType.CANCEL).hasNoLeaks();
          }
        }

        List<Signal<Payload>> values = assertSubscriber.values();
        for (int j = 0; j < values.size(); j++) {
          Signal<Payload> signal = values.get(j);

          if (signal.isOnNext()) {
            PayloadAssert.assertThat(signal.get())
                .describedAs("Expected that the next signal[%s] to have no leaks", j)
                .hasNoLeaks();
          } else {
            if (inboundTerminationMode.equals("error")) {
              Assertions.assertThat(signal.isOnError()).isTrue();
              Throwable throwable = signal.getThrowable();
              if (throwable == inboundException) {
                Assertions.assertThat(droppedErrors.get(0))
                    .isExactlyInstanceOf(
                        outboundTerminationMode.equals("onError")
                            ? outboundException.getClass()
                            : outboundTerminationMode.equals("refCntError")
                                ? IllegalReferenceCountException.class
                                : IllegalArgumentException.class);
                Assertions.assertThat(throwable).isEqualTo(inboundException);
              } else {
                Assertions.assertThat(droppedErrors).containsOnly(inboundException);
                Assertions.assertThat(throwable)
                    .isExactlyInstanceOf(
                        outboundTerminationMode.equals("onError")
                            ? outboundException.getClass()
                            : outboundTerminationMode.equals("refCntError")
                                ? IllegalReferenceCountException.class
                                : IllegalArgumentException.class);
              }
            } else if (inboundTerminationMode.equals("complete")) {
              if (signal.isOnComplete()) {
                Assertions.assertThat(droppedErrors.get(0))
                    .isExactlyInstanceOf(
                        outboundTerminationMode.equals("onError")
                            ? outboundException.getClass()
                            : outboundTerminationMode.equals("refCntError")
                                ? IllegalReferenceCountException.class
                                : IllegalArgumentException.class);
              } else {
                Assertions.assertThat(droppedErrors).isEmpty();
                Assertions.assertThat(signal.getThrowable())
                    .isExactlyInstanceOf(
                        outboundTerminationMode.equals("onError")
                            ? outboundException.getClass()
                            : outboundTerminationMode.equals("refCntError")
                                ? IllegalReferenceCountException.class
                                : IllegalArgumentException.class);
              }
            } else {
              Assertions.assertThat(signal.getThrowable())
                  .isExactlyInstanceOf(
                      outboundTerminationMode.equals("onError")
                          ? outboundException.getClass()
                          : outboundTerminationMode.equals("refCntError")
                              ? IllegalReferenceCountException.class
                              : IllegalArgumentException.class);
            }

            Assertions.assertThat(j)
                .describedAs(
                    "Expected that the error signal[%s] is the last signal, but the last was %s",
                    j, values.size() - 1)
                .isEqualTo(values.size() - 1);
          }
        }

        allocator.assertHasNoLeaks();
        droppedErrors.clear();
      }
    } finally {
      Hooks.resetOnErrorDropped();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"complete", "cancel"})
  public void shouldRemoveItselfFromActiveStreamsWhenInboundAndOutboundAreTerminated(
      String outboundTerminationMode) {
    for (int i = 0; i < 10000; i++) {
      final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
      final LeaksTrackingByteBufAllocator allocator = activeStreams.getAllocator();
      final TestDuplexConnection sender = activeStreams.getDuplexConnection();
      final TestPublisher<Payload> publisher =
          TestPublisher.createNoncompliant(TestPublisher.Violation.DEFER_CANCELLATION);

      final RequestChannelRequesterFlux requestChannelRequesterFlux =
          new RequestChannelRequesterFlux(publisher, activeStreams);
      final StateAssert<RequestChannelRequesterFlux> stateAssert =
          StateAssert.assertThat(requestChannelRequesterFlux);

      stateAssert.isUnsubscribed();
      activeStreams.assertNoActiveStreams();

      final AssertSubscriber<Signal<Payload>> assertSubscriber =
          requestChannelRequesterFlux.materialize().subscribeWith(AssertSubscriber.create(0));
      activeStreams.assertNoActiveStreams();

      // state machine check
      stateAssert.hasSubscribedFlagOnly();

      assertSubscriber.request(Integer.MAX_VALUE);
      stateAssert.hasSubscribedFlag().hasRequestN(Integer.MAX_VALUE).hasNoFirstFrameSentFlag();
      activeStreams.assertNoActiveStreams();

      Payload requestPayload = TestRequesterResponderSupport.randomPayload(allocator);
      publisher.next(requestPayload);

      stateAssert.hasSubscribedFlag().hasRequestN(Integer.MAX_VALUE).hasFirstFrameSentFlag();

      activeStreams.assertHasStream(1, requestChannelRequesterFlux);
      FrameAssert.assertThat(sender.awaitFrame())
          .typeOf(FrameType.REQUEST_CHANNEL)
          .hasRequestN(Integer.MAX_VALUE)
          .hasNoLeaks();

      requestChannelRequesterFlux.handleRequestN(Long.MAX_VALUE);

      RaceTestUtils.race(
          () -> {
            if (outboundTerminationMode.equals("cancel")) {
              requestChannelRequesterFlux.handleCancel();
            } else {
              publisher.complete();
            }
          },
          requestChannelRequesterFlux::handleComplete);

      ByteBuf completeFrameOrNull = sender.pollFrame();
      if (completeFrameOrNull != null) {
        FrameAssert.assertThat(completeFrameOrNull)
            .hasStreamId(1)
            .typeOf(FrameType.COMPLETE)
            .hasNoLeaks();
      }

      assertSubscriber.assertTerminated().assertComplete();
      activeStreams.assertNoActiveStreams();
      allocator.assertHasNoLeaks();
    }
  }
}
