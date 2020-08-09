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

import static io.rsocket.frame.FrameType.METADATA_PUSH;
import static io.rsocket.frame.FrameType.NEXT;
import static io.rsocket.frame.FrameType.REQUEST_CHANNEL;
import static io.rsocket.frame.FrameType.REQUEST_FNF;
import static io.rsocket.frame.FrameType.REQUEST_RESPONSE;

import io.netty.buffer.ByteBuf;
import io.rsocket.FrameAssert;
import io.rsocket.Payload;
import io.rsocket.PayloadAssert;
import io.rsocket.RSocket;
import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
import io.rsocket.frame.FrameType;
import io.rsocket.internal.subscriber.AssertSubscriber;
import io.rsocket.test.util.TestDuplexConnection;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.assertj.core.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.test.publisher.TestPublisher;

public class ResponderOperatorsCommonTest {

  interface Scenario {
    FrameType requestType();

    int maxElements();

    ResponderFrameHandler responseOperator(
        long initialRequestN,
        Payload firstPayload,
        TestRequesterResponderSupport streamManager,
        RSocket handler);

    ResponderFrameHandler responseOperator(
        long initialRequestN,
        ByteBuf firstFragment,
        TestRequesterResponderSupport streamManager,
        RSocket handler);
  }

  static Stream<Scenario> scenarios() {
    return Stream.of(
        new Scenario() {
          @Override
          public FrameType requestType() {
            return FrameType.REQUEST_RESPONSE;
          }

          @Override
          public int maxElements() {
            return 1;
          }

          @Override
          public ResponderFrameHandler responseOperator(
              long initialRequestN,
              ByteBuf firstFragment,
              TestRequesterResponderSupport streamManager,
              RSocket handler) {
            int streamId = streamManager.getNextStreamId();
            RequestResponseResponderSubscriber subscriber =
                new RequestResponseResponderSubscriber(
                    streamId, firstFragment, streamManager, handler);
            streamManager.activeStreams.put(streamId, subscriber);
            return subscriber;
          }

          @Override
          public ResponderFrameHandler responseOperator(
              long initialRequestN,
              Payload firstPayload,
              TestRequesterResponderSupport streamManager,
              RSocket handler) {
            int streamId = streamManager.getNextStreamId();
            RequestResponseResponderSubscriber subscriber =
                new RequestResponseResponderSubscriber(streamId, streamManager);
            streamManager.activeStreams.put(streamId, subscriber);
            return handler.requestResponse(firstPayload).subscribeWith(subscriber);
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
          public int maxElements() {
            return Integer.MAX_VALUE;
          }

          @Override
          public ResponderFrameHandler responseOperator(
              long initialRequestN,
              ByteBuf firstFragment,
              TestRequesterResponderSupport streamManager,
              RSocket handler) {
            int streamId = streamManager.getNextStreamId();
            RequestStreamResponderSubscriber subscriber =
                new RequestStreamResponderSubscriber(
                    streamId, initialRequestN, firstFragment, streamManager, handler);
            streamManager.activeStreams.put(streamId, subscriber);
            return subscriber;
          }

          @Override
          public ResponderFrameHandler responseOperator(
              long initialRequestN,
              Payload firstPayload,
              TestRequesterResponderSupport streamManager,
              RSocket handler) {
            int streamId = streamManager.getNextStreamId();
            RequestStreamResponderSubscriber subscriber =
                new RequestStreamResponderSubscriber(streamId, initialRequestN, streamManager);
            streamManager.activeStreams.put(streamId, subscriber);
            return handler.requestStream(firstPayload).subscribeWith(subscriber);
          }

          @Override
          public String toString() {
            return RequestStreamResponderSubscriber.class.getSimpleName();
          }
        },
        new Scenario() {
          @Override
          public FrameType requestType() {
            return FrameType.REQUEST_CHANNEL;
          }

          @Override
          public int maxElements() {
            return Integer.MAX_VALUE;
          }

          @Override
          public ResponderFrameHandler responseOperator(
              long initialRequestN,
              ByteBuf firstFragment,
              TestRequesterResponderSupport streamManager,
              RSocket handler) {
            int streamId = streamManager.getNextStreamId();
            RequestChannelResponderSubscriber subscriber =
                new RequestChannelResponderSubscriber(
                    streamId, initialRequestN, firstFragment, streamManager, handler);
            streamManager.activeStreams.put(streamId, subscriber);
            return subscriber;
          }

          @Override
          public ResponderFrameHandler responseOperator(
              long initialRequestN,
              Payload firstPayload,
              TestRequesterResponderSupport streamManager,
              RSocket handler) {
            int streamId = streamManager.getNextStreamId();
            RequestChannelResponderSubscriber responderSubscriber =
                new RequestChannelResponderSubscriber(
                    streamId, initialRequestN, firstPayload, streamManager);
            streamManager.activeStreams.put(streamId, responderSubscriber);
            return handler.requestChannel(responderSubscriber).subscribeWith(responderSubscriber);
          }

          @Override
          public String toString() {
            return RequestChannelResponderSubscriber.class.getSimpleName();
          }
        });
  }

  static class TestHandler implements RSocket {

    final TestPublisher<Payload> producer;
    final AssertSubscriber<Payload> consumer;

    TestHandler(TestPublisher<Payload> producer, AssertSubscriber<Payload> consumer) {
      this.producer = producer;
      this.consumer = consumer;
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
      consumer.onSubscribe(Operators.emptySubscription());
      consumer.onNext(payload);
      consumer.onComplete();
      return producer.mono().then();
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
      consumer.onSubscribe(Operators.emptySubscription());
      consumer.onNext(payload);
      consumer.onComplete();
      return producer.mono();
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
      consumer.onSubscribe(Operators.emptySubscription());
      consumer.onNext(payload);
      consumer.onComplete();
      return producer.flux();
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
      payloads.subscribe(consumer);
      return producer.flux();
    }
  }

  @ParameterizedTest
  @MethodSource("scenarios")
  void shouldHandleRequest(Scenario scenario) {
    Assumptions.assumeThat(scenario.requestType()).isNotIn(REQUEST_FNF, METADATA_PUSH);

    TestRequesterResponderSupport testRequesterResponderSupport =
        TestRequesterResponderSupport.client();
    final LeaksTrackingByteBufAllocator allocator = testRequesterResponderSupport.getAllocator();
    final TestDuplexConnection sender = testRequesterResponderSupport.getDuplexConnection();
    TestPublisher<Payload> testPublisher = TestPublisher.create();
    TestHandler testHandler = new TestHandler(testPublisher, new AssertSubscriber<>(0));

    ResponderFrameHandler responderFrameHandler =
        scenario.responseOperator(
            Long.MAX_VALUE,
            TestRequesterResponderSupport.genericPayload(allocator),
            testRequesterResponderSupport,
            testHandler);

    Payload randomPayload = TestRequesterResponderSupport.randomPayload(allocator);
    testPublisher.assertWasSubscribed();
    testPublisher.next(randomPayload.retain());
    testPublisher.complete();

    FrameAssert.assertThat(sender.awaitFrame())
        .isNotNull()
        .hasStreamId(1)
        .typeOf(scenario.requestType() == REQUEST_RESPONSE ? FrameType.NEXT_COMPLETE : NEXT)
        .hasPayloadSize(
            randomPayload.data().readableBytes() + randomPayload.sliceMetadata().readableBytes())
        .hasData(randomPayload.data())
        .hasNoLeaks();

    PayloadAssert.assertThat(randomPayload).hasNoLeaks();

    if (scenario.requestType() != REQUEST_RESPONSE) {

      FrameAssert.assertThat(sender.awaitFrame())
          .typeOf(FrameType.COMPLETE)
          .hasStreamId(1)
          .hasNoLeaks();

      if (scenario.requestType() == REQUEST_CHANNEL) {
        testHandler.consumer.request(2);
        FrameAssert.assertThat(sender.awaitFrame())
            .typeOf(FrameType.REQUEST_N)
            .hasStreamId(1)
            .hasRequestN(1)
            .hasNoLeaks();
      }
    }

    testHandler
        .consumer
        .assertValueCount(1)
        .assertValuesWith(p -> PayloadAssert.assertThat(p).hasNoLeaks());

    allocator.assertHasNoLeaks();
  }

  @ParameterizedTest
  @MethodSource("scenarios")
  void shouldHandleFragmentedRequest(Scenario scenario) {
    Assumptions.assumeThat(scenario.requestType()).isNotIn(REQUEST_FNF, METADATA_PUSH);

    TestRequesterResponderSupport testRequesterResponderSupport =
        TestRequesterResponderSupport.client();
    final LeaksTrackingByteBufAllocator allocator = testRequesterResponderSupport.getAllocator();
    final TestDuplexConnection sender = testRequesterResponderSupport.getDuplexConnection();
    TestPublisher<Payload> testPublisher = TestPublisher.create();
    TestHandler testHandler = new TestHandler(testPublisher, new AssertSubscriber<>(0));

    int mtu = ThreadLocalRandom.current().nextInt(64, 256);
    Payload firstPayload = TestRequesterResponderSupport.randomPayload(allocator);
    ArrayList<ByteBuf> fragments =
        TestRequesterResponderSupport.prepareFragments(allocator, mtu, firstPayload);

    ByteBuf firstFragment = fragments.remove(0);
    ResponderFrameHandler responderFrameHandler =
        scenario.responseOperator(
            Long.MAX_VALUE, firstFragment, testRequesterResponderSupport, testHandler);
    firstFragment.release();

    testPublisher.assertWasNotSubscribed();
    testRequesterResponderSupport.assertHasStream(1, responderFrameHandler);

    for (int i = 0; i < fragments.size(); i++) {
      ByteBuf fragment = fragments.get(i);
      boolean hasFollows = i != fragments.size() - 1;
      responderFrameHandler.handleNext(fragment, hasFollows, !hasFollows);
      fragment.release();
    }

    Payload randomPayload = TestRequesterResponderSupport.randomPayload(allocator);
    testPublisher.assertWasSubscribed();
    testPublisher.next(randomPayload.retain());
    testPublisher.complete();

    FrameAssert.assertThat(sender.awaitFrame())
        .isNotNull()
        .hasStreamId(1)
        .typeOf(scenario.requestType() == REQUEST_RESPONSE ? FrameType.NEXT_COMPLETE : NEXT)
        .hasPayloadSize(
            randomPayload.data().readableBytes() + randomPayload.sliceMetadata().readableBytes())
        .hasData(randomPayload.data())
        .hasNoLeaks();

    PayloadAssert.assertThat(randomPayload).hasNoLeaks();

    if (scenario.requestType() != REQUEST_RESPONSE) {

      FrameAssert.assertThat(sender.awaitFrame())
          .typeOf(FrameType.COMPLETE)
          .hasStreamId(1)
          .hasNoLeaks();

      if (scenario.requestType() == REQUEST_CHANNEL) {
        testHandler.consumer.request(2);
        FrameAssert.assertThat(sender.pollFrame()).isNull();
      }
    }

    testHandler
        .consumer
        .assertValueCount(1)
        .assertValuesWith(
            p -> PayloadAssert.assertThat(p).hasData(firstPayload.sliceData()).hasNoLeaks())
        .assertComplete();

    testRequesterResponderSupport.assertNoActiveStreams();

    firstPayload.release();

    allocator.assertHasNoLeaks();
  }

  @ParameterizedTest
  @MethodSource("scenarios")
  void shouldHandleInterruptedFragmentation(Scenario scenario) {
    Assumptions.assumeThat(scenario.requestType()).isNotIn(REQUEST_FNF, METADATA_PUSH);

    TestRequesterResponderSupport testRequesterResponderSupport =
        TestRequesterResponderSupport.client();
    final LeaksTrackingByteBufAllocator allocator = testRequesterResponderSupport.getAllocator();
    TestPublisher<Payload> testPublisher = TestPublisher.create();
    TestHandler testHandler = new TestHandler(testPublisher, new AssertSubscriber<>(0));

    int mtu = ThreadLocalRandom.current().nextInt(64, 256);
    Payload firstPayload = TestRequesterResponderSupport.randomPayload(allocator);
    ArrayList<ByteBuf> fragments =
        TestRequesterResponderSupport.prepareFragments(allocator, mtu, firstPayload);
    firstPayload.release();

    ByteBuf firstFragment = fragments.remove(0);
    ResponderFrameHandler responderFrameHandler =
        scenario.responseOperator(
            Long.MAX_VALUE, firstFragment, testRequesterResponderSupport, testHandler);
    firstFragment.release();

    testPublisher.assertWasNotSubscribed();
    testRequesterResponderSupport.assertHasStream(1, responderFrameHandler);

    for (int i = 0; i < fragments.size(); i++) {
      ByteBuf fragment = fragments.get(i);
      boolean hasFollows = i != fragments.size() - 1;
      if (hasFollows) {
        responderFrameHandler.handleNext(fragment, true, false);
      } else {
        responderFrameHandler.handleCancel();
      }
      fragment.release();
    }

    testPublisher.assertWasNotSubscribed();
    testRequesterResponderSupport.assertNoActiveStreams();

    allocator.assertHasNoLeaks();
  }
}
