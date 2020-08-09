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

import static io.rsocket.frame.FrameType.COMPLETE;
import static io.rsocket.frame.FrameType.METADATA_PUSH;
import static io.rsocket.frame.FrameType.REQUEST_CHANNEL;
import static io.rsocket.frame.FrameType.REQUEST_N;
import static io.rsocket.frame.FrameType.REQUEST_RESPONSE;
import static io.rsocket.frame.FrameType.REQUEST_STREAM;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.CharsetUtil;
import io.rsocket.FrameAssert;
import io.rsocket.Payload;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.RequestStreamFrameCodec;
import io.rsocket.internal.subscriber.AssertSubscriber;
import io.rsocket.util.ByteBufPayload;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.test.StepVerifier;
import reactor.test.util.RaceTestUtils;

@SuppressWarnings("ALL")
public class RequesterOperatorsRacingTest {

  interface Scenario {
    FrameType requestType();

    Publisher<?> requestOperator(
        Supplier<Payload> payloadsSupplier, RequesterResponderSupport requesterResponderSupport);
  }

  static Stream<Scenario> scenarios() {
    return Stream.of(
        new Scenario() {
          @Override
          public FrameType requestType() {
            return METADATA_PUSH;
          }

          @Override
          public Publisher<?> requestOperator(
              Supplier<Payload> payloadsSupplier,
              RequesterResponderSupport requesterResponderSupport) {
            return new MetadataPushRequesterMono(payloadsSupplier.get(), requesterResponderSupport);
          }

          @Override
          public String toString() {
            return MetadataPushRequesterMono.class.getSimpleName();
          }
        },
        new Scenario() {
          @Override
          public FrameType requestType() {
            return FrameType.REQUEST_FNF;
          }

          @Override
          public Publisher<?> requestOperator(
              Supplier<Payload> payloadsSupplier,
              RequesterResponderSupport requesterResponderSupport) {
            return new FireAndForgetRequesterMono(
                payloadsSupplier.get(), requesterResponderSupport);
          }

          @Override
          public String toString() {
            return FireAndForgetRequesterMono.class.getSimpleName();
          }
        },
        new Scenario() {
          @Override
          public FrameType requestType() {
            return FrameType.REQUEST_RESPONSE;
          }

          @Override
          public Publisher<?> requestOperator(
              Supplier<Payload> payloadsSupplier,
              RequesterResponderSupport requesterResponderSupport) {
            return new RequestResponseRequesterMono(
                payloadsSupplier.get(), requesterResponderSupport);
          }

          @Override
          public String toString() {
            return RequestResponseRequesterMono.class.getSimpleName();
          }
        },
        new Scenario() {
          @Override
          public FrameType requestType() {
            return FrameType.REQUEST_STREAM;
          }

          @Override
          public Publisher<?> requestOperator(
              Supplier<Payload> payloadsSupplier,
              RequesterResponderSupport requesterResponderSupport) {
            return new RequestStreamRequesterFlux(
                payloadsSupplier.get(), requesterResponderSupport);
          }

          @Override
          public String toString() {
            return RequestStreamRequesterFlux.class.getSimpleName();
          }
        },
        new Scenario() {
          @Override
          public FrameType requestType() {
            return FrameType.REQUEST_CHANNEL;
          }

          @Override
          public Publisher<?> requestOperator(
              Supplier<Payload> payloadsSupplier,
              RequesterResponderSupport requesterResponderSupport) {
            return new RequestChannelRequesterFlux(
                Flux.generate(s -> s.next(payloadsSupplier.get())), requesterResponderSupport);
          }

          @Override
          public String toString() {
            return RequestChannelRequesterFlux.class.getSimpleName();
          }
        });
  }

  /*
   * +--------------------------------+
   * |       Racing Test Cases        |
   * +--------------------------------+
   */

  /** Ensures single subscription happens in case of racing */
  @ParameterizedTest(name = "Should subscribe exactly once to {0}")
  @MethodSource("scenarios")
  public void shouldSubscribeExactlyOnce(Scenario scenario) {
    for (int i = 0; i < 10000; i++) {
      final TestRequesterResponderSupport requesterResponderSupport =
          TestRequesterResponderSupport.client();
      final Supplier<Payload> payloadSupplier =
          () ->
              TestRequesterResponderSupport.genericPayload(
                  requesterResponderSupport.getAllocator());

      final Publisher<?> requestOperator =
          scenario.requestOperator(payloadSupplier, requesterResponderSupport);

      StepVerifier stepVerifier =
          StepVerifier.create(requesterResponderSupport.getDuplexConnection().getSentAsPublisher())
              .assertNext(
                  frame -> {
                    FrameAssert frameAssert =
                        FrameAssert.assertThat(frame)
                            .isNotNull()
                            .hasNoFragmentsFollow()
                            .typeOf(scenario.requestType());
                    if (scenario.requestType() == METADATA_PUSH) {
                      frameAssert
                          .hasStreamIdZero()
                          .hasPayloadSize(
                              TestRequesterResponderSupport.METADATA_CONTENT.getBytes(
                                      CharsetUtil.UTF_8)
                                  .length)
                          .hasMetadata(TestRequesterResponderSupport.METADATA_CONTENT);
                    } else {
                      frameAssert
                          .hasClientSideStreamId()
                          .hasStreamId(1)
                          .hasPayloadSize(
                              TestRequesterResponderSupport.METADATA_CONTENT.getBytes(
                                          CharsetUtil.UTF_8)
                                      .length
                                  + TestRequesterResponderSupport.DATA_CONTENT.getBytes(
                                          CharsetUtil.UTF_8)
                                      .length)
                          .hasMetadata(TestRequesterResponderSupport.METADATA_CONTENT)
                          .hasData(TestRequesterResponderSupport.DATA_CONTENT);
                    }
                    frameAssert.hasNoLeaks();

                    if (requestOperator instanceof FrameHandler) {
                      ((FrameHandler) requestOperator).handleComplete();
                    }
                  })
              .thenCancel()
              .verifyLater();

      Assertions.assertThatThrownBy(
              () ->
                  RaceTestUtils.race(
                      () -> {
                        AssertSubscriber subscriber = new AssertSubscriber<>();
                        requestOperator.subscribe(subscriber);
                        subscriber.await().assertTerminated().assertNoError();
                      },
                      () -> {
                        AssertSubscriber subscriber = new AssertSubscriber<>();
                        requestOperator.subscribe(subscriber);
                        subscriber.await().assertTerminated().assertNoError();
                      }))
          .matches(
              t -> {
                Assertions.assertThat(t).hasMessageContaining("allows only a single Subscriber");
                return true;
              });

      stepVerifier.verify(Duration.ofSeconds(1));
      requesterResponderSupport.getAllocator().assertHasNoLeaks();
    }
  }

  /** Ensures single frame is sent only once racing between requests */
  @ParameterizedTest(name = "{0} should sent requestFrame exactly once if request(n) is racing")
  @MethodSource("scenarios")
  public void shouldSentRequestFrameOnceInCaseOfRequestRacing(Scenario scenario) {
    Assumptions.assumeThat(scenario.requestType())
        .isIn(REQUEST_RESPONSE, REQUEST_STREAM, REQUEST_CHANNEL);

    for (int i = 0; i < 10000; i++) {
      final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
      final Supplier<Payload> payloadSupplier =
          () -> TestRequesterResponderSupport.genericPayload(activeStreams.getAllocator());

      final Publisher<Payload> requestOperator =
          (Publisher<Payload>) scenario.requestOperator(payloadSupplier, activeStreams);

      Payload response = ByteBufPayload.create("test", "test");

      final AssertSubscriber<Payload> assertSubscriber = AssertSubscriber.create(0);
      requestOperator.subscribe(assertSubscriber);

      RaceTestUtils.race(() -> assertSubscriber.request(1), () -> assertSubscriber.request(1));

      final ByteBuf sentFrame = activeStreams.getDuplexConnection().awaitFrame();

      if (scenario.requestType().hasInitialRequestN()) {
        if (RequestStreamFrameCodec.initialRequestN(sentFrame) == 1) {
          FrameAssert.assertThat(activeStreams.getDuplexConnection().awaitFrame())
              .isNotNull()
              .hasStreamId(1)
              .hasRequestN(1)
              .typeOf(REQUEST_N)
              .hasNoLeaks();
        } else {
          Assertions.assertThat(RequestStreamFrameCodec.initialRequestN(sentFrame)).isEqualTo(2);
        }
      }

      FrameAssert.assertThat(sentFrame)
          .isNotNull()
          .hasPayloadSize(
              TestRequesterResponderSupport.DATA_CONTENT.getBytes(CharsetUtil.UTF_8).length
                  + TestRequesterResponderSupport.METADATA_CONTENT.getBytes(CharsetUtil.UTF_8)
                      .length)
          .hasMetadata(TestRequesterResponderSupport.METADATA_CONTENT)
          .hasData(TestRequesterResponderSupport.DATA_CONTENT)
          .hasNoFragmentsFollow()
          .typeOf(scenario.requestType())
          .hasClientSideStreamId()
          .hasStreamId(1)
          .hasNoLeaks();

      ((RequesterFrameHandler) requestOperator).handlePayload(response);
      ((RequesterFrameHandler) requestOperator).handleComplete();

      if (scenario.requestType() == REQUEST_CHANNEL) {
        ((CoreSubscriber) requestOperator).onComplete();
        FrameAssert.assertThat(activeStreams.getDuplexConnection().awaitFrame())
            .typeOf(COMPLETE)
            .hasStreamId(1)
            .hasNoLeaks();
      }

      assertSubscriber
          .assertTerminated()
          .assertValuesWith(
              p -> {
                Assertions.assertThat(p.release()).isTrue();
                Assertions.assertThat(p.refCnt()).isZero();
              });

      activeStreams.assertNoActiveStreams();
      Assertions.assertThat(activeStreams.getDuplexConnection().isEmpty()).isTrue();
      activeStreams.getAllocator().assertHasNoLeaks();
    }
  }

  /**
   * Ensures that no ByteBuf is leaked if reassembly is starting and cancel is happening at the same
   * time
   */
  @ParameterizedTest(name = "Should have no leaks when {0} is canceled during reassembly")
  @MethodSource("scenarios")
  public void shouldHaveNoLeaksOnReassemblyAndCancelRacing(Scenario scenario) {
    Assumptions.assumeThat(scenario.requestType())
        .isIn(REQUEST_RESPONSE, REQUEST_STREAM, REQUEST_CHANNEL);

    for (int i = 0; i < 10000; i++) {
      final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
      final Supplier<Payload> payloadSupplier =
          () -> TestRequesterResponderSupport.genericPayload(activeStreams.getAllocator());

      final Publisher<Payload> requestOperator =
          (Publisher<Payload>) scenario.requestOperator(payloadSupplier, activeStreams);

      final AssertSubscriber<Payload> assertSubscriber = new AssertSubscriber<>(1);

      requestOperator.subscribe(assertSubscriber);

      final ByteBuf sentFrame = activeStreams.getDuplexConnection().awaitFrame();
      FrameAssert.assertThat(sentFrame)
          .isNotNull()
          .hasPayloadSize(
              TestRequesterResponderSupport.DATA_CONTENT.getBytes(CharsetUtil.UTF_8).length
                  + TestRequesterResponderSupport.METADATA_CONTENT.getBytes(CharsetUtil.UTF_8)
                      .length)
          .hasMetadata(TestRequesterResponderSupport.METADATA_CONTENT)
          .hasData(TestRequesterResponderSupport.DATA_CONTENT)
          .hasNoFragmentsFollow()
          .typeOf(scenario.requestType())
          .hasClientSideStreamId()
          .hasStreamId(1)
          .hasNoLeaks();

      int mtu = ThreadLocalRandom.current().nextInt(64, 256);
      Payload responsePayload =
          TestRequesterResponderSupport.randomPayload(activeStreams.getAllocator());
      ArrayList<ByteBuf> fragments =
          TestRequesterResponderSupport.prepareFragments(
              activeStreams.getAllocator(), mtu, responsePayload);
      RaceTestUtils.race(
          assertSubscriber::cancel,
          () -> {
            FrameHandler frameHandler = (FrameHandler) requestOperator;
            int lastFragmentId = fragments.size() - 1;
            for (int j = 0; j < fragments.size(); j++) {
              ByteBuf frame = fragments.get(j);
              frameHandler.handleNext(frame, lastFragmentId != j, lastFragmentId == j);
              frame.release();
            }
          });

      List<Payload> values = assertSubscriber.values();
      if (!values.isEmpty()) {
        Assertions.assertThat(values)
            .hasSize(1)
            .first()
            .matches(
                p -> {
                  Assertions.assertThat(p.sliceData())
                      .matches(bb -> ByteBufUtil.equals(bb, responsePayload.sliceData()));
                  Assertions.assertThat(p.hasMetadata()).isEqualTo(responsePayload.hasMetadata());
                  Assertions.assertThat(p.sliceMetadata())
                      .matches(bb -> ByteBufUtil.equals(bb, responsePayload.sliceMetadata()));
                  Assertions.assertThat(p.release()).isTrue();
                  Assertions.assertThat(p.refCnt()).isZero();
                  return true;
                });
      }

      if (!activeStreams.getDuplexConnection().isEmpty()) {
        if (scenario.requestType() != REQUEST_CHANNEL) {
          assertSubscriber.assertNotTerminated();
        }

        final ByteBuf cancellationFrame = activeStreams.getDuplexConnection().awaitFrame();
        FrameAssert.assertThat(cancellationFrame)
            .isNotNull()
            .typeOf(FrameType.CANCEL)
            .hasClientSideStreamId()
            .hasStreamId(1)
            .hasNoLeaks();
      }

      Assertions.assertThat(responsePayload.release()).isTrue();
      Assertions.assertThat(responsePayload.refCnt()).isZero();

      activeStreams.assertNoActiveStreams();
      Assertions.assertThat(activeStreams.getDuplexConnection().isEmpty()).isTrue();
      activeStreams.getAllocator().assertHasNoLeaks();
    }
  }

  /**
   * Ensures that in case of racing between next element and cancel we will not have any memory
   * leaks
   */
  @Test
  public void shouldHaveNoLeaksOnNextAndCancelRacing() {
    for (int i = 0; i < 10000; i++) {
      final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
      final Payload payload =
          TestRequesterResponderSupport.genericPayload(activeStreams.getAllocator());

      final RequestResponseRequesterMono requestResponseRequesterMono =
          new RequestResponseRequesterMono(payload, activeStreams);

      Payload response = ByteBufPayload.create("test", "test");

      StepVerifier.create(requestResponseRequesterMono.doOnNext(Payload::release))
          .expectSubscription()
          .expectComplete()
          .verifyLater();

      final ByteBuf sentFrame = activeStreams.getDuplexConnection().awaitFrame();
      FrameAssert.assertThat(sentFrame)
          .isNotNull()
          .hasPayloadSize(
              TestRequesterResponderSupport.DATA_CONTENT.getBytes(CharsetUtil.UTF_8).length
                  + TestRequesterResponderSupport.METADATA_CONTENT.getBytes(CharsetUtil.UTF_8)
                      .length)
          .hasMetadata(TestRequesterResponderSupport.METADATA_CONTENT)
          .hasData(TestRequesterResponderSupport.DATA_CONTENT)
          .hasNoFragmentsFollow()
          .typeOf(FrameType.REQUEST_RESPONSE)
          .hasClientSideStreamId()
          .hasStreamId(1)
          .hasNoLeaks();

      RaceTestUtils.race(
          requestResponseRequesterMono::cancel,
          () -> requestResponseRequesterMono.handlePayload(response));

      Assertions.assertThat(payload.refCnt()).isZero();
      Assertions.assertThat(response.refCnt()).isZero();

      activeStreams.assertNoActiveStreams();
      final boolean isEmpty = activeStreams.getDuplexConnection().isEmpty();
      if (!isEmpty) {
        final ByteBuf cancellationFrame = activeStreams.getDuplexConnection().awaitFrame();
        FrameAssert.assertThat(cancellationFrame)
            .isNotNull()
            .typeOf(FrameType.CANCEL)
            .hasClientSideStreamId()
            .hasStreamId(1)
            .hasNoLeaks();
      }
      Assertions.assertThat(activeStreams.getDuplexConnection().isEmpty()).isTrue();

      StateAssert.assertThat(requestResponseRequesterMono).isTerminated();
      activeStreams.getAllocator().assertHasNoLeaks();
    }
  }

  /**
   * Ensures that in case we have element reassembling and then it happens the remote sends
   * (errorFrame) and downstream subscriber sends cancel() and we have racing between onError and
   * cancel we will not have any memory leaks
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void shouldHaveNoUnexpectedErrorDuringOnErrorAndCancelRacing(boolean withReassembly) {
    final ArrayList<Throwable> droppedErrors = new ArrayList<>();
    Hooks.onErrorDropped(droppedErrors::add);
    try {
      for (int i = 0; i < 10000; i++) {
        final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
        final Payload payload =
            TestRequesterResponderSupport.genericPayload(activeStreams.getAllocator());

        final RequestResponseRequesterMono requestResponseRequesterMono =
            new RequestResponseRequesterMono(payload, activeStreams);

        final StateAssert<RequestResponseRequesterMono> stateAssert =
            StateAssert.assertThat(requestResponseRequesterMono);

        stateAssert.isUnsubscribed();
        final AssertSubscriber<Payload> assertSubscriber =
            requestResponseRequesterMono.subscribeWith(AssertSubscriber.create(0));

        stateAssert.hasSubscribedFlagOnly();

        assertSubscriber.request(1);

        stateAssert.hasSubscribedFlag().hasRequestN(1).hasFirstFrameSentFlag();

        final ByteBuf sentFrame = activeStreams.getDuplexConnection().awaitFrame();
        FrameAssert.assertThat(sentFrame)
            .isNotNull()
            .hasPayloadSize(
                TestRequesterResponderSupport.DATA_CONTENT.getBytes(CharsetUtil.UTF_8).length
                    + TestRequesterResponderSupport.METADATA_CONTENT.getBytes(CharsetUtil.UTF_8)
                        .length)
            .hasMetadata(TestRequesterResponderSupport.METADATA_CONTENT)
            .hasData(TestRequesterResponderSupport.DATA_CONTENT)
            .hasNoFragmentsFollow()
            .typeOf(FrameType.REQUEST_RESPONSE)
            .hasClientSideStreamId()
            .hasStreamId(1)
            .hasNoLeaks();

        if (withReassembly) {
          final ByteBuf fragmentBuf =
              activeStreams.getAllocator().buffer().writeBytes(new byte[] {1, 2, 3});
          requestResponseRequesterMono.handleNext(fragmentBuf, true, false);
          // mimic frameHandler behaviour
          fragmentBuf.release();
        }

        final RuntimeException testException = new RuntimeException("test");
        RaceTestUtils.race(
            requestResponseRequesterMono::cancel,
            () -> requestResponseRequesterMono.handleError(testException));

        Assertions.assertThat(payload.refCnt()).isZero();

        activeStreams.assertNoActiveStreams();
        stateAssert.isTerminated();

        final boolean isEmpty = activeStreams.getDuplexConnection().isEmpty();
        if (!isEmpty) {
          final ByteBuf cancellationFrame = activeStreams.getDuplexConnection().awaitFrame();
          FrameAssert.assertThat(cancellationFrame)
              .isNotNull()
              .typeOf(FrameType.CANCEL)
              .hasClientSideStreamId()
              .hasStreamId(1)
              .hasNoLeaks();

          Assertions.assertThat(droppedErrors).containsExactly(testException);
        } else {
          assertSubscriber.assertTerminated().assertErrorMessage("test");
        }
        Assertions.assertThat(activeStreams.getDuplexConnection().isEmpty()).isTrue();

        stateAssert.isTerminated();
        droppedErrors.clear();
        activeStreams.getAllocator().assertHasNoLeaks();
      }
    } finally {
      Hooks.resetOnErrorDropped();
    }
  }

  /**
   * Ensures that in case of racing between first request and cancel does not going to introduce
   * leaks. <br>
   * <br>
   *
   * <p>Please note, first request may or may not happen so in case it happened before cancellation
   * signal we have to observe
   *
   * <ul>
   *   <li>RequestResponseFrame
   *   <li>CancellationFrame
   * </ul>
   *
   * <p>exactly in that order
   *
   * <p>Ensures full serialization of outgoing signal (frames)
   */
  @Test
  public void shouldBeConsistentInCaseOfRacingOfCancellationAndRequest() {
    for (int i = 0; i < 10000; i++) {
      final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
      final Payload payload =
          TestRequesterResponderSupport.genericPayload(activeStreams.getAllocator());

      final RequestResponseRequesterMono requestResponseRequesterMono =
          new RequestResponseRequesterMono(payload, activeStreams);

      Payload response = ByteBufPayload.create("test", "test");

      final AssertSubscriber<Payload> assertSubscriber =
          requestResponseRequesterMono.subscribeWith(new AssertSubscriber<>(0));

      RaceTestUtils.race(() -> assertSubscriber.cancel(), () -> assertSubscriber.request(1));

      if (!activeStreams.getDuplexConnection().isEmpty()) {
        final ByteBuf sentFrame = activeStreams.getDuplexConnection().awaitFrame();
        FrameAssert.assertThat(sentFrame)
            .isNotNull()
            .typeOf(FrameType.REQUEST_RESPONSE)
            .hasPayloadSize(
                TestRequesterResponderSupport.DATA_CONTENT.getBytes(CharsetUtil.UTF_8).length
                    + TestRequesterResponderSupport.METADATA_CONTENT.getBytes(CharsetUtil.UTF_8)
                        .length)
            .hasMetadata(TestRequesterResponderSupport.METADATA_CONTENT)
            .hasData(TestRequesterResponderSupport.DATA_CONTENT)
            .hasNoFragmentsFollow()
            .hasClientSideStreamId()
            .hasStreamId(1)
            .hasNoLeaks();

        final ByteBuf cancelFrame = activeStreams.getDuplexConnection().awaitFrame();
        FrameAssert.assertThat(cancelFrame)
            .isNotNull()
            .typeOf(FrameType.CANCEL)
            .hasClientSideStreamId()
            .hasStreamId(1)
            .hasNoLeaks();
      }

      Assertions.assertThat(payload.refCnt()).isZero();

      StateAssert.assertThat(requestResponseRequesterMono).isTerminated();

      requestResponseRequesterMono.handlePayload(response);
      Assertions.assertThat(response.refCnt()).isZero();

      activeStreams.assertNoActiveStreams();
      Assertions.assertThat(activeStreams.getDuplexConnection().isEmpty()).isTrue();
      activeStreams.getAllocator().assertHasNoLeaks();
    }
  }

  /** Ensures that CancelFrame is sent exactly once in case of racing between cancel() methods */
  @Test
  public void shouldSentCancelFrameExactlyOnce() {
    for (int i = 0; i < 10000; i++) {
      final TestRequesterResponderSupport activeStreams = TestRequesterResponderSupport.client();
      final Payload payload =
          TestRequesterResponderSupport.genericPayload(activeStreams.getAllocator());

      final RequestResponseRequesterMono requestResponseRequesterMono =
          new RequestResponseRequesterMono(payload, activeStreams);

      Payload response = ByteBufPayload.create("test", "test");

      final AssertSubscriber<Payload> assertSubscriber =
          requestResponseRequesterMono.subscribeWith(new AssertSubscriber<>(0));

      assertSubscriber.request(1);

      final ByteBuf sentFrame = activeStreams.getDuplexConnection().awaitFrame();
      FrameAssert.assertThat(sentFrame)
          .isNotNull()
          .hasNoFragmentsFollow()
          .typeOf(FrameType.REQUEST_RESPONSE)
          .hasClientSideStreamId()
          .hasPayloadSize(
              TestRequesterResponderSupport.DATA_CONTENT.getBytes(CharsetUtil.UTF_8).length
                  + TestRequesterResponderSupport.METADATA_CONTENT.getBytes(CharsetUtil.UTF_8)
                      .length)
          .hasMetadata(TestRequesterResponderSupport.METADATA_CONTENT)
          .hasData(TestRequesterResponderSupport.DATA_CONTENT)
          .hasStreamId(1)
          .hasNoLeaks();

      RaceTestUtils.race(
          requestResponseRequesterMono::cancel, requestResponseRequesterMono::cancel);

      final ByteBuf cancelFrame = activeStreams.getDuplexConnection().awaitFrame();
      FrameAssert.assertThat(cancelFrame)
          .isNotNull()
          .typeOf(FrameType.CANCEL)
          .hasClientSideStreamId()
          .hasStreamId(1)
          .hasNoLeaks();

      Assertions.assertThat(payload.refCnt()).isZero();
      activeStreams.assertNoActiveStreams();

      StateAssert.assertThat(requestResponseRequesterMono).isTerminated();

      requestResponseRequesterMono.handlePayload(response);
      Assertions.assertThat(response.refCnt()).isZero();

      requestResponseRequesterMono.handleComplete();
      assertSubscriber.assertNotTerminated();

      activeStreams.assertNoActiveStreams();
      Assertions.assertThat(activeStreams.getDuplexConnection().isEmpty()).isTrue();
      activeStreams.getAllocator().assertHasNoLeaks();
    }
  }
}
