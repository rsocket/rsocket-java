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

import static io.rsocket.core.PayloadValidationUtils.INVALID_PAYLOAD_ERROR_MESSAGE;
import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;
import static io.rsocket.frame.FrameType.*;
import static reactor.test.publisher.TestPublisher.Violation.CLEANUP_ON_TERMINATE;
import static reactor.test.publisher.TestPublisher.Violation.DEFER_CANCELLATION;

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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.Exceptions;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Signal;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.util.RaceTestUtils;

public class RequestChannelResponderSubscriberTest {

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
  @ValueSource(strings = {"inbound", "outbound", "inboundCancel"})
  public void requestNFrameShouldBeSentOnSubscriptionAndThenSeparately(String completionCase) {
    final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
    LeaksTrackingByteBufAllocator allocator = activeStreams.getAllocator();
    final TestDuplexConnection sender = activeStreams.getDuplexConnection();
    final Payload firstPayload = TestRequesterResponderSupport.genericPayload(allocator);
    final TestPublisher<Payload> publisher = TestPublisher.create();

    final RequestChannelResponderSubscriber requestChannelResponderSubscriber =
        new RequestChannelResponderSubscriber(1, 1, firstPayload, activeStreams);
    final StateAssert<RequestChannelResponderSubscriber> stateAssert =
        StateAssert.assertThat(requestChannelResponderSubscriber);
    activeStreams.activeStreams.put(1, requestChannelResponderSubscriber);

    // state machine check
    stateAssert.isUnsubscribed().hasRequestN(0);
    activeStreams.assertHasStream(1, requestChannelResponderSubscriber);

    publisher.subscribe(requestChannelResponderSubscriber);
    publisher.assertMaxRequested(1);
    // state machine check
    stateAssert.isUnsubscribed().hasRequestN(0);

    final AssertSubscriber<Payload> assertSubscriber =
        requestChannelResponderSubscriber.subscribeWith(AssertSubscriber.create(0));
    Assertions.assertThat(firstPayload.refCnt()).isOne();

    // state machine check
    stateAssert.hasSubscribedFlagOnly().hasRequestN(0);

    assertSubscriber.request(1);

    // state machine check
    stateAssert.hasSubscribedFlag().hasFirstFrameSentFlag().hasRequestN(1);

    // should not send requestN since 1 is remaining
    Assertions.assertThat(sender.isEmpty()).isTrue();

    assertSubscriber.request(1);

    stateAssert.hasSubscribedFlag().hasRequestN(2).hasFirstFrameSentFlag();

    // should not send requestN since 1 is remaining
    FrameAssert.assertThat(sender.awaitFrame())
        .typeOf(REQUEST_N)
        .hasStreamId(1)
        .hasRequestN(1)
        .hasNoLeaks();

    publisher.next(TestRequesterResponderSupport.genericPayload(allocator));

    final ByteBuf frame = sender.awaitFrame();
    FrameAssert.assertThat(frame)
        .isNotNull()
        .hasPayloadSize(
            "testData".getBytes(CharsetUtil.UTF_8).length
                + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
        .hasMetadata("testMetadata")
        .hasData("testData")
        .hasNoFragmentsFollow()
        .typeOf(NEXT)
        .hasClientSideStreamId()
        .hasStreamId(1)
        .hasNoLeaks();

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

    Payload nextPayload = TestRequesterResponderSupport.genericPayload(allocator);
    requestChannelResponderSubscriber.handlePayload(nextPayload);

    int mtu = ThreadLocalRandom.current().nextInt(64, 256);
    Payload randomPayload = TestRequesterResponderSupport.randomPayload(allocator);
    ArrayList<ByteBuf> fragments =
        TestRequesterResponderSupport.prepareFragments(allocator, mtu, randomPayload);

    ByteBuf firstFragment = fragments.remove(0);
    requestChannelResponderSubscriber.handleNext(firstFragment, true, false);
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

      requestChannelResponderSubscriber.handleNext(followingFragment, hasFollows, false);
      followingFragment.release();
    }

    // state machine check
    stateAssert
        .hasSubscribedFlag()
        .hasRequestN(Integer.MAX_VALUE)
        .hasFirstFrameSentFlag()
        .hasNoReassemblingFlag();

    if (completionCase.equals("inbound")) {
      requestChannelResponderSubscriber.handleComplete();
      assertSubscriber
          .assertValuesWith(
              p -> PayloadAssert.assertThat(p).isSameAs(firstPayload).hasNoLeaks(),
              p -> PayloadAssert.assertThat(p).isSameAs(nextPayload).hasNoLeaks(),
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
    } else if (completionCase.equals("inboundCancel")) {
      assertSubscriber.cancel();
      assertSubscriber.assertValuesWith(
          p -> PayloadAssert.assertThat(p).isSameAs(firstPayload).hasNoLeaks(),
          p -> PayloadAssert.assertThat(p).isSameAs(nextPayload).hasNoLeaks(),
          p -> {
            PayloadAssert.assertThat(p).isEqualTo(randomPayload).hasNoLeaks();
            randomPayload.release();
          });

      FrameAssert.assertThat(sender.awaitFrame()).typeOf(CANCEL).hasStreamId(1).hasNoLeaks();

      // state machine check
      stateAssert
          .hasSubscribedFlag()
          .hasRequestN(Integer.MAX_VALUE)
          .hasFirstFrameSentFlag()
          .hasNoReassemblingFlag()
          .hasInboundTerminated();

      publisher.complete();
      FrameAssert.assertThat(sender.awaitFrame())
          .typeOf(FrameType.COMPLETE)
          .hasStreamId(1)
          .hasNoLeaks();
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

      requestChannelResponderSubscriber.handleComplete();
      assertSubscriber
          .assertValuesWith(
              p -> PayloadAssert.assertThat(p).isSameAs(p).hasNoLeaks(),
              p -> PayloadAssert.assertThat(p).isEqualTo(nextPayload).hasNoLeaks(),
              p -> {
                PayloadAssert.assertThat(p).isEqualTo(randomPayload).hasNoLeaks();
                randomPayload.release();
              })
          .assertComplete();
    }

    Assertions.assertThat(firstPayload.refCnt()).isZero();
    stateAssert.isTerminated();
    activeStreams.assertNoActiveStreams();

    Assertions.assertThat(sender.isEmpty()).isTrue();
    allocator.assertHasNoLeaks();
  }

  /*
   * +--------------------------------+
   * |       Racing Test Cases        |
   * +--------------------------------+
   */

  @Test
  public void streamShouldWorkCorrectlyWhenRacingHandleCompleteWithSubscription() {
    for (int i = 0; i < 10000; i++) {
      final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
      final LeaksTrackingByteBufAllocator allocator = activeStreams.getAllocator();
      final TestDuplexConnection sender = activeStreams.getDuplexConnection();
      ;
      final Payload firstPayload = TestRequesterResponderSupport.randomPayload(allocator);
      final TestPublisher<Payload> publisher = TestPublisher.create();

      final RequestChannelResponderSubscriber requestChannelResponderSubscriber =
          new RequestChannelResponderSubscriber(1, 1, firstPayload, activeStreams);
      final StateAssert<RequestChannelResponderSubscriber> stateAssert =
          StateAssert.assertThat(requestChannelResponderSubscriber);
      activeStreams.activeStreams.put(1, requestChannelResponderSubscriber);

      // state machine check
      stateAssert.isUnsubscribed();
      activeStreams.assertHasStream(1, requestChannelResponderSubscriber);

      publisher.subscribe(requestChannelResponderSubscriber);

      final AssertSubscriber<Payload> assertSubscriber = AssertSubscriber.create(1);

      RaceTestUtils.race(
          () ->
              requestChannelResponderSubscriber
                  .doOnNext(__ -> assertSubscriber.request(1))
                  .subscribe(assertSubscriber),
          () -> requestChannelResponderSubscriber.handleComplete());

      stateAssert
          .hasSubscribedFlag()
          .hasInboundTerminated()
          .hasFirstFrameSentFlag()
          .hasRequestNBetween(1, 2);

      assertSubscriber
          .assertValuesWith(p -> PayloadAssert.assertThat(p).isSameAs(firstPayload).hasNoLeaks())
          .assertTerminated()
          .assertComplete();

      publisher.complete();

      if (sender.getSent().size() > 1) {
        FrameAssert.assertThat(sender.awaitFrame())
            .hasStreamId(1)
            .typeOf(REQUEST_N)
            .hasRequestN(1)
            .hasNoLeaks();
      }
      FrameAssert.assertThat(sender.awaitFrame()).hasStreamId(1).typeOf(COMPLETE).hasNoLeaks();

      // state machine check
      stateAssert.isTerminated();
      allocator.assertHasNoLeaks();
    }
  }

  @Test
  public void streamShouldWorkCorrectlyWhenRacingHandleErrorWithSubscription() {
    ApplicationErrorException applicationErrorException = new ApplicationErrorException("test");

    for (int i = 0; i < 10000; i++) {
      final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
      final LeaksTrackingByteBufAllocator allocator = activeStreams.getAllocator();
      final Payload firstPayload = TestRequesterResponderSupport.randomPayload(allocator);
      final TestPublisher<Payload> publisher = TestPublisher.create();

      final RequestChannelResponderSubscriber requestChannelResponderSubscriber =
          new RequestChannelResponderSubscriber(1, 1, firstPayload, activeStreams);
      final StateAssert<RequestChannelResponderSubscriber> stateAssert =
          StateAssert.assertThat(requestChannelResponderSubscriber);
      activeStreams.activeStreams.put(1, requestChannelResponderSubscriber);

      // state machine check
      stateAssert.isUnsubscribed();
      activeStreams.assertHasStream(1, requestChannelResponderSubscriber);

      publisher.subscribe(requestChannelResponderSubscriber);

      final AssertSubscriber<Payload> assertSubscriber = AssertSubscriber.create(1);

      RaceTestUtils.race(
          () -> requestChannelResponderSubscriber.subscribe(assertSubscriber),
          () -> requestChannelResponderSubscriber.handleError(applicationErrorException));

      stateAssert.isTerminated();

      publisher.assertCancelled(1);

      if (!assertSubscriber.values().isEmpty()) {
        assertSubscriber.assertValuesWith(
            p -> PayloadAssert.assertThat(p).isSameAs(p).hasNoLeaks());
      }

      assertSubscriber
          .assertTerminated()
          .assertError(applicationErrorException.getClass())
          .assertErrorMessage("test");

      allocator.assertHasNoLeaks();
    }
  }

  @Test
  public void streamShouldWorkCorrectlyWhenRacingHandleCancelWithSubscription() {
    for (int i = 0; i < 10000; i++) {
      final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
      final LeaksTrackingByteBufAllocator allocator = activeStreams.getAllocator();
      final Payload firstPayload = TestRequesterResponderSupport.randomPayload(allocator);
      final TestPublisher<Payload> publisher = TestPublisher.create();

      final RequestChannelResponderSubscriber requestChannelResponderSubscriber =
          new RequestChannelResponderSubscriber(1, 1, firstPayload, activeStreams);
      final StateAssert<RequestChannelResponderSubscriber> stateAssert =
          StateAssert.assertThat(requestChannelResponderSubscriber);
      activeStreams.activeStreams.put(1, requestChannelResponderSubscriber);

      // state machine check
      stateAssert.isUnsubscribed();
      activeStreams.assertHasStream(1, requestChannelResponderSubscriber);

      publisher.subscribe(requestChannelResponderSubscriber);

      final AssertSubscriber<Payload> assertSubscriber = AssertSubscriber.create(1);

      RaceTestUtils.race(
          () -> requestChannelResponderSubscriber.subscribe(assertSubscriber),
          () -> requestChannelResponderSubscriber.handleCancel());

      stateAssert.isTerminated();

      publisher.assertCancelled(1);

      if (!assertSubscriber.values().isEmpty()) {
        assertSubscriber.assertValuesWith(
            p -> PayloadAssert.assertThat(p).isSameAs(p).hasNoLeaks());
      }

      assertSubscriber
          .assertTerminated()
          .assertError(CancellationException.class)
          .assertErrorMessage("Inbound has been canceled");

      allocator.assertHasNoLeaks();
    }
  }

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
            TestPublisher.createNoncompliant(DEFER_CANCELLATION, CLEANUP_ON_TERMINATE);

        Payload requestPayload = TestRequesterResponderSupport.randomPayload(allocator);
        final RequestChannelResponderSubscriber requestChannelResponderSubscriber =
            new RequestChannelResponderSubscriber(1, 1, requestPayload, activeStreams);

        activeStreams.activeStreams.put(1, requestChannelResponderSubscriber);

        publisher.subscribe(requestChannelResponderSubscriber);
        final AssertSubscriber<Signal<Payload>> assertSubscriber =
            requestChannelResponderSubscriber
                .materialize()
                .subscribeWith(AssertSubscriber.create(0));

        assertSubscriber.request(Integer.MAX_VALUE);

        FrameAssert.assertThat(sender.awaitFrame())
            .typeOf(FrameType.REQUEST_N)
            .hasRequestN(Integer.MAX_VALUE)
            .hasNoLeaks();

        requestChannelResponderSubscriber.handleRequestN(Long.MAX_VALUE);

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
              requestChannelResponderSubscriber.handlePayload(responsePayload1);
              requestChannelResponderSubscriber.handlePayload(responsePayload2);
              requestChannelResponderSubscriber.handlePayload(responsePayload3);

              if (inboundTerminationMode.equals("error")) {
                requestChannelResponderSubscriber.handleError(inboundException);
              } else if (inboundTerminationMode.equals("complete")) {
                requestChannelResponderSubscriber.handleComplete();
              } else {
                requestChannelResponderSubscriber.handleCancel();
              }
            });

        ByteBuf errorFrameOrEmpty = sender.pollFrame();
        if (errorFrameOrEmpty != null) {
          String message;
          if (outboundTerminationMode.equals("onError")) {
            message = outboundException.getMessage();
          } else if (outboundTerminationMode.equals("sizeError")) {
            message = String.format(INVALID_PAYLOAD_ERROR_MESSAGE, FRAME_LENGTH_MASK);
          } else {
            message = "Failed to validate payload. Cause:refCnt: 0";
          }
          FrameAssert.assertThat(errorFrameOrEmpty)
              .typeOf(FrameType.ERROR)
              .hasData(message)
              .hasNoLeaks();
        }

        List<Signal<Payload>> values = assertSubscriber.values();
        for (int j = 0; j < values.size(); j++) {
          Signal<Payload> signal = values.get(j);

          if (signal.isOnNext()) {
            Payload payload = signal.get();
            if (j == 0) {
              Assertions.assertThat(payload).isEqualTo(requestPayload);
            }

            PayloadAssert.assertThat(payload)
                .describedAs("Expected that the next signal[%s] to have no leaks", j)
                .hasNoLeaks();
          } else {
            if (inboundTerminationMode.equals("error")) {
              Assertions.assertThat(signal.isOnError()).isTrue();
              Throwable throwable = signal.getThrowable();
              if (Exceptions.isMultiple(throwable)) {
                Assertions.assertThat(
                        Arrays.stream(throwable.getSuppressed()).map(Throwable::getMessage))
                    .containsExactlyInAnyOrder(
                        inboundException.getMessage(),
                        outboundTerminationMode.equals("onError")
                            ? "Outbound has terminated with an error"
                            : "Inbound has been canceled");
              } else {
                if (throwable == inboundException) {
                  Assertions.assertThat(droppedErrors)
                      .hasSize(1)
                      .first()
                      .isExactlyInstanceOf(
                          outboundTerminationMode.equals("onError")
                              ? outboundException.getClass()
                              : outboundTerminationMode.equals("refCntError")
                                  ? IllegalReferenceCountException.class
                                  : IllegalArgumentException.class);
                } else {
                  Assertions.assertThat(droppedErrors).containsOnly(inboundException);
                }
              }
            } else if (inboundTerminationMode.equals("complete")) {
              Assertions.assertThat(droppedErrors).isEmpty();
              if (signal.isOnError()) {
                Assertions.assertThat(signal.getThrowable())
                    .isExactlyInstanceOf(CancellationException.class)
                    .matches(
                        t ->
                            t.getMessage().equals("Inbound has been canceled")
                                || t.getMessage().equals("Outbound has terminated with an error"));
              }
            } else {
              Throwable throwable = signal.getThrowable();
              if (Exceptions.isMultiple(throwable)) {
                Assertions.assertThat(
                        Arrays.stream(throwable.getSuppressed()).map(Throwable::getMessage))
                    .containsExactlyInAnyOrder(
                        "Inbound has been canceled",
                        outboundTerminationMode.equals("onError")
                            ? "Outbound has terminated with an error"
                            : "Inbound has been canceled");
              } else {
                Assertions.assertThat(throwable).isExactlyInstanceOf(CancellationException.class);
              }
            }

            Assertions.assertThat(j)
                .describedAs(
                    "Expected that the %s signal[%s] is the last signal, but the last was %s",
                    signal, j, values.get(values.size() - 1))
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
  @ValueSource(strings = {"onError", "sizeError", "refCntError", "cancel"})
  public void shouldHaveNoLeaksOnReassemblyAndCancelRacing(String terminationMode) {
    final RuntimeException outboundException = new RuntimeException("outboundException");
    final Payload oversizePayload =
        DefaultPayload.create(new byte[FRAME_LENGTH_MASK], new byte[FRAME_LENGTH_MASK]);

    for (int i = 0; i < 10000; i++) {
      final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
      final LeaksTrackingByteBufAllocator allocator = activeStreams.getAllocator();
      final TestDuplexConnection sender = activeStreams.getDuplexConnection();
      ;
      final TestPublisher<Payload> publisher =
          TestPublisher.createNoncompliant(DEFER_CANCELLATION, CLEANUP_ON_TERMINATE);
      final AssertSubscriber<Payload> assertSubscriber = new AssertSubscriber<>(1);

      Payload firstPayload = TestRequesterResponderSupport.genericPayload(allocator);
      final RequestChannelResponderSubscriber requestOperator =
          new RequestChannelResponderSubscriber(1, Long.MAX_VALUE, firstPayload, activeStreams);

      publisher.subscribe(requestOperator);
      requestOperator.subscribe(assertSubscriber);

      int mtu = ThreadLocalRandom.current().nextInt(64, 256);
      Payload responsePayload = TestRequesterResponderSupport.randomPayload(allocator);
      ArrayList<ByteBuf> fragments =
          TestRequesterResponderSupport.prepareFragments(allocator, mtu, responsePayload);

      Payload releasedPayload1 = ByteBufPayload.create(new byte[0]);
      Payload releasedPayload2 = ByteBufPayload.create(new byte[0]);
      releasedPayload1.release();
      releasedPayload2.release();

      RaceTestUtils.race(
          () -> {
            switch (terminationMode) {
              case "onError":
                publisher.error(outboundException);
                break;
              case "sizeError":
                publisher.next(oversizePayload);
                break;
              case "refCntError":
                publisher.next(releasedPayload1);
                break;
              case "cancel":
              default:
                assertSubscriber.cancel();
            }
          },
          () -> {
            int lastFragmentId = fragments.size() - 1;
            for (int j = 0; j < fragments.size(); j++) {
              ByteBuf frame = fragments.get(j);
              requestOperator.handleNext(frame, lastFragmentId != j, false);
              frame.release();
            }
          });

      List<Payload> values = assertSubscriber.values();

      PayloadAssert.assertThat(values.get(0)).isEqualTo(firstPayload).hasNoLeaks();

      if (values.size() > 1) {
        Payload payload = values.get(1);
        PayloadAssert.assertThat(payload).isEqualTo(responsePayload).hasNoLeaks();
      }

      if (!sender.isEmpty()) {
        if (terminationMode.equals("cancel")) {
          assertSubscriber.assertNotTerminated();
        } else {
          assertSubscriber.assertTerminated().assertError();
        }

        final ByteBuf frame = sender.awaitFrame();
        FrameAssert.assertThat(frame)
            .isNotNull()
            .typeOf(terminationMode.equals("cancel") ? CANCEL : ERROR)
            .hasClientSideStreamId()
            .hasStreamId(1)
            .hasNoLeaks();
      }

      PayloadAssert.assertThat(responsePayload).hasNoLeaks();

      activeStreams.assertNoActiveStreams();
      Assertions.assertThat(sender.isEmpty()).isTrue();
      allocator.assertHasNoLeaks();
    }
  }
}
