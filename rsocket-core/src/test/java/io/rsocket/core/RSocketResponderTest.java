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

package io.rsocket.core;

import static io.rsocket.core.PayloadValidationUtils.INVALID_PAYLOAD_ERROR_MESSAGE;
import static io.rsocket.core.ReassemblyUtils.ILLEGAL_REASSEMBLED_PAYLOAD_SIZE;
import static io.rsocket.core.TestRequesterResponderSupport.fixedSizePayload;
import static io.rsocket.core.TestRequesterResponderSupport.genericPayload;
import static io.rsocket.core.TestRequesterResponderSupport.prepareFragments;
import static io.rsocket.core.TestRequesterResponderSupport.randomMetadataOnlyPayload;
import static io.rsocket.core.TestRequesterResponderSupport.randomPayload;
import static io.rsocket.frame.FrameHeaderCodec.frameType;
import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;
import static io.rsocket.frame.FrameType.COMPLETE;
import static io.rsocket.frame.FrameType.ERROR;
import static io.rsocket.frame.FrameType.NEXT;
import static io.rsocket.frame.FrameType.NEXT_COMPLETE;
import static io.rsocket.frame.FrameType.REQUEST_CHANNEL;
import static io.rsocket.frame.FrameType.REQUEST_FNF;
import static io.rsocket.frame.FrameType.REQUEST_N;
import static io.rsocket.frame.FrameType.REQUEST_RESPONSE;
import static io.rsocket.frame.FrameType.REQUEST_STREAM;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.rsocket.FrameAssert;
import io.rsocket.Payload;
import io.rsocket.PayloadAssert;
import io.rsocket.RSocket;
import io.rsocket.frame.CancelFrameCodec;
import io.rsocket.frame.ErrorFrameCodec;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.KeepAliveFrameCodec;
import io.rsocket.frame.PayloadFrameCodec;
import io.rsocket.frame.RequestChannelFrameCodec;
import io.rsocket.frame.RequestFireAndForgetFrameCodec;
import io.rsocket.frame.RequestNFrameCodec;
import io.rsocket.frame.RequestResponseFrameCodec;
import io.rsocket.frame.RequestStreamFrameCodec;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.subscriber.AssertSubscriber;
import io.rsocket.lease.ResponderLeaseHandler;
import io.rsocket.test.util.TestDuplexConnection;
import io.rsocket.test.util.TestSubscriber;
import io.rsocket.util.ByteBufPayload;
import io.rsocket.util.DefaultPayload;
import io.rsocket.util.EmptyPayload;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.runners.model.Statement;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.publisher.TestPublisher;
import reactor.test.util.RaceTestUtils;

public class RSocketResponderTest {

  ServerSocketRule rule;

  @BeforeEach
  public void setUp() throws Throwable {
    Hooks.onNextDropped(ReferenceCountUtil::safeRelease);
    Hooks.onErrorDropped(t -> {});
    rule = new ServerSocketRule();
    rule.apply(
            new Statement() {
              @Override
              public void evaluate() {}
            },
            null)
        .evaluate();
  }

  @AfterEach
  public void tearDown() {
    Hooks.resetOnErrorDropped();
    Hooks.resetOnNextDropped();
  }

  @Test
  @Timeout(2_000)
  @Disabled
  public void testHandleKeepAlive() throws Exception {
    rule.connection.addToReceivedBuffer(
        KeepAliveFrameCodec.encode(rule.alloc(), true, 0, Unpooled.EMPTY_BUFFER));
    ByteBuf sent = rule.connection.awaitFrame();
    assertThat("Unexpected frame sent.", frameType(sent), is(FrameType.KEEPALIVE));
    /*Keep alive ack must not have respond flag else, it will result in infinite ping-pong of keep alive frames.*/
    assertThat(
        "Unexpected keep-alive frame respond flag.",
        KeepAliveFrameCodec.respondFlag(sent),
        is(false));
  }

  @Test
  @Timeout(2_000)
  public void testHandleResponseFrameNoError() throws Exception {
    final int streamId = 4;
    rule.connection.clearSendReceiveBuffers();
    final TestPublisher<Payload> testPublisher = TestPublisher.create();
    rule.setAcceptingSocket(
        new RSocket() {
          @Override
          public Mono<Payload> requestResponse(Payload payload) {
            return testPublisher.mono();
          }
        });
    rule.sendRequest(streamId, FrameType.REQUEST_RESPONSE);
    testPublisher.complete();
    assertThat(
        "Unexpected frame sent.",
        frameType(rule.connection.awaitFrame()),
        anyOf(is(FrameType.COMPLETE), is(FrameType.NEXT_COMPLETE)));
    testPublisher.assertWasNotCancelled();
  }

  @Test
  @Timeout(2_000)
  @Disabled
  public void testHandlerEmitsError() throws Exception {
    final int streamId = 4;
    rule.sendRequest(streamId, FrameType.REQUEST_STREAM);
    assertThat(
        "Unexpected frame sent.", frameType(rule.connection.awaitFrame()), is(FrameType.ERROR));
  }

  @Test
  @Timeout(20_000)
  public void testCancel() {
    ByteBufAllocator allocator = rule.alloc();
    final int streamId = 4;
    final AtomicBoolean cancelled = new AtomicBoolean();
    rule.setAcceptingSocket(
        new RSocket() {
          @Override
          public Mono<Payload> requestResponse(Payload payload) {
            payload.release();
            return Mono.<Payload>never().doOnCancel(() -> cancelled.set(true));
          }
        });
    rule.sendRequest(streamId, FrameType.REQUEST_RESPONSE);

    assertThat("Unexpected frame sent.", rule.connection.getSent(), is(empty()));

    rule.connection.addToReceivedBuffer(CancelFrameCodec.encode(allocator, streamId));

    assertThat("Unexpected frame sent.", rule.connection.getSent(), is(empty()));
    assertThat("Subscription not cancelled.", cancelled.get(), is(true));
    rule.assertHasNoLeaks();
  }

  @ParameterizedTest
  @ValueSource(ints = {128, 256, FRAME_LENGTH_MASK})
  @Timeout(2_000)
  public void shouldThrownExceptionIfGivenPayloadIsExitsSizeAllowanceWithNoFragmentation(
      int maxFrameLength) {
    rule.setMaxFrameLength(maxFrameLength);
    final int streamId = 4;
    final AtomicBoolean cancelled = new AtomicBoolean();
    byte[] metadata = new byte[maxFrameLength];
    byte[] data = new byte[maxFrameLength];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);
    final Payload payload = DefaultPayload.create(data, metadata);
    final RSocket acceptingSocket =
        new RSocket() {
          @Override
          public Mono<Payload> requestResponse(Payload p) {
            p.release();
            return Mono.just(payload).doOnCancel(() -> cancelled.set(true));
          }

          @Override
          public Flux<Payload> requestStream(Payload p) {
            p.release();
            return Flux.just(payload).doOnCancel(() -> cancelled.set(true));
          }

          @Override
          public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
            Flux.from(payloads)
                .doOnNext(Payload::release)
                .subscribe(
                    new BaseSubscriber<Payload>() {
                      @Override
                      protected void hookOnSubscribe(Subscription subscription) {
                        subscription.request(1);
                      }
                    });
            return Flux.just(payload).doOnCancel(() -> cancelled.set(true));
          }
        };
    rule.setAcceptingSocket(acceptingSocket);

    final Runnable[] runnables = {
      () -> rule.sendRequest(streamId, FrameType.REQUEST_RESPONSE),
      () -> rule.sendRequest(streamId, FrameType.REQUEST_STREAM),
      () -> rule.sendRequest(streamId, FrameType.REQUEST_CHANNEL)
    };

    for (Runnable runnable : runnables) {
      rule.connection.clearSendReceiveBuffers();
      runnable.run();
      Assertions.assertThat(rule.connection.getSent())
          .hasSize(1)
          .first()
          .matches(bb -> FrameHeaderCodec.frameType(bb) == FrameType.ERROR)
          .matches(
              bb ->
                  ErrorFrameCodec.dataUtf8(bb)
                      .contains(String.format(INVALID_PAYLOAD_ERROR_MESSAGE, maxFrameLength)))
          .matches(ReferenceCounted::release);

      assertThat("Subscription not cancelled.", cancelled.get(), is(true));
    }

    rule.assertHasNoLeaks();
  }

  @Test
  public void checkNoLeaksOnRacingCancelFromRequestChannelAndNextFromUpstream() {
    ByteBufAllocator allocator = rule.alloc();
    for (int i = 0; i < 10000; i++) {
      AssertSubscriber<Payload> assertSubscriber = AssertSubscriber.create();

      rule.setAcceptingSocket(
          new RSocket() {
            @Override
            public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
              payloads.subscribe(assertSubscriber);
              return Flux.never();
            }
          },
          Integer.MAX_VALUE);

      rule.sendRequest(1, REQUEST_CHANNEL);

      ByteBuf metadata1 = allocator.buffer();
      metadata1.writeCharSequence("abc1", CharsetUtil.UTF_8);
      ByteBuf data1 = allocator.buffer();
      data1.writeCharSequence("def1", CharsetUtil.UTF_8);
      ByteBuf nextFrame1 =
          PayloadFrameCodec.encode(allocator, 1, false, false, true, metadata1, data1);

      ByteBuf metadata2 = allocator.buffer();
      metadata2.writeCharSequence("abc2", CharsetUtil.UTF_8);
      ByteBuf data2 = allocator.buffer();
      data2.writeCharSequence("def2", CharsetUtil.UTF_8);
      ByteBuf nextFrame2 =
          PayloadFrameCodec.encode(allocator, 1, false, false, true, metadata2, data2);

      ByteBuf metadata3 = allocator.buffer();
      metadata3.writeCharSequence("abc3", CharsetUtil.UTF_8);
      ByteBuf data3 = allocator.buffer();
      data3.writeCharSequence("def3", CharsetUtil.UTF_8);
      ByteBuf nextFrame3 =
          PayloadFrameCodec.encode(allocator, 1, false, false, true, metadata3, data3);

      RaceTestUtils.race(
          () -> {
            rule.connection.addToReceivedBuffer(nextFrame1, nextFrame2, nextFrame3);
          },
          assertSubscriber::cancel);

      Assertions.assertThat(assertSubscriber.values()).allMatch(ReferenceCounted::release);

      Assertions.assertThat(rule.connection.getSent()).allMatch(ReferenceCounted::release);

      rule.assertHasNoLeaks();
    }
  }

  @Test
  public void checkNoLeaksOnRacingBetweenDownstreamCancelAndOnNextFromRequestChannelTest() {
    Hooks.onErrorDropped((e) -> {});
    ByteBufAllocator allocator = rule.alloc();
    for (int i = 0; i < 10000; i++) {
      AssertSubscriber<Payload> assertSubscriber = AssertSubscriber.create();

      FluxSink<Payload>[] sinks = new FluxSink[1];

      rule.setAcceptingSocket(
          new RSocket() {
            @Override
            public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
              ((Flux<Payload>) payloads)
                  .doOnNext(ReferenceCountUtil::safeRelease)
                  .subscribe(assertSubscriber);
              return Flux.create(sink -> sinks[0] = sink, FluxSink.OverflowStrategy.IGNORE);
            }
          },
          1);

      rule.sendRequest(1, REQUEST_CHANNEL);

      ByteBuf cancelFrame = CancelFrameCodec.encode(allocator, 1);
      FluxSink<Payload> sink = sinks[0];
      RaceTestUtils.race(
          () -> rule.connection.addToReceivedBuffer(cancelFrame),
          () -> {
            sink.next(ByteBufPayload.create("d1", "m1"));
            sink.next(ByteBufPayload.create("d2", "m2"));
            sink.next(ByteBufPayload.create("d3", "m3"));
          });

      Assertions.assertThat(rule.connection.getSent()).allMatch(ReferenceCounted::release);

      rule.assertHasNoLeaks();
    }
  }

  @Test
  public void checkNoLeaksOnRacingBetweenDownstreamCancelAndOnNextFromRequestChannelTest1() {
    Scheduler parallel = Schedulers.parallel();
    Hooks.onErrorDropped((e) -> {});
    ByteBufAllocator allocator = rule.alloc();
    for (int i = 0; i < 10000; i++) {
      AssertSubscriber<Payload> assertSubscriber = AssertSubscriber.create();

      FluxSink<Payload>[] sinks = new FluxSink[1];

      rule.setAcceptingSocket(
          new RSocket() {
            @Override
            public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
              ((Flux<Payload>) payloads)
                  .doOnNext(ReferenceCountUtil::safeRelease)
                  .subscribe(assertSubscriber);
              return Flux.create(sink -> sinks[0] = sink, FluxSink.OverflowStrategy.IGNORE);
            }
          },
          1);

      rule.sendRequest(1, REQUEST_CHANNEL);

      ByteBuf cancelFrame = CancelFrameCodec.encode(allocator, 1);
      ByteBuf requestNFrame = RequestNFrameCodec.encode(allocator, 1, Integer.MAX_VALUE);
      FluxSink<Payload> sink = sinks[0];
      RaceTestUtils.race(
          () ->
              RaceTestUtils.race(
                  () -> rule.connection.addToReceivedBuffer(requestNFrame),
                  () -> rule.connection.addToReceivedBuffer(cancelFrame),
                  parallel),
          () -> {
            sink.next(ByteBufPayload.create("d1", "m1"));
            sink.next(ByteBufPayload.create("d2", "m2"));
            sink.next(ByteBufPayload.create("d3", "m3"));
          },
          parallel);

      Assertions.assertThat(rule.connection.getSent()).allMatch(ReferenceCounted::release);

      rule.assertHasNoLeaks();
    }
  }

  @Test
  public void
      checkNoLeaksOnRacingBetweenDownstreamCancelAndOnNextFromUpstreamOnErrorFromRequestChannelTest1() {
    Scheduler parallel = Schedulers.parallel();
    Hooks.onErrorDropped((e) -> {});
    ByteBufAllocator allocator = rule.alloc();
    for (int i = 0; i < 10000; i++) {
      FluxSink<Payload>[] sinks = new FluxSink[1];
      AssertSubscriber<Payload> assertSubscriber = AssertSubscriber.create();
      rule.setAcceptingSocket(
          new RSocket() {
            @Override
            public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
              payloads.subscribe(assertSubscriber);

              return Flux.<Payload>create(
                  sink -> {
                    sinks[0] = sink;
                  },
                  FluxSink.OverflowStrategy.IGNORE);
            }
          },
          1);

      rule.sendRequest(1, REQUEST_CHANNEL);

      ByteBuf metadata1 = allocator.buffer();
      metadata1.writeCharSequence("abc1", CharsetUtil.UTF_8);
      ByteBuf data1 = allocator.buffer();
      data1.writeCharSequence("def1", CharsetUtil.UTF_8);
      ByteBuf nextFrame1 =
          PayloadFrameCodec.encode(allocator, 1, false, false, true, metadata1, data1);

      ByteBuf metadata2 = allocator.buffer();
      metadata2.writeCharSequence("abc2", CharsetUtil.UTF_8);
      ByteBuf data2 = allocator.buffer();
      data2.writeCharSequence("def2", CharsetUtil.UTF_8);
      ByteBuf nextFrame2 =
          PayloadFrameCodec.encode(allocator, 1, false, false, true, metadata2, data2);

      ByteBuf metadata3 = allocator.buffer();
      metadata3.writeCharSequence("abc3", CharsetUtil.UTF_8);
      ByteBuf data3 = allocator.buffer();
      data3.writeCharSequence("def3", CharsetUtil.UTF_8);
      ByteBuf nextFrame3 =
          PayloadFrameCodec.encode(allocator, 1, false, false, true, metadata3, data3);

      ByteBuf requestNFrame = RequestNFrameCodec.encode(allocator, 1, Integer.MAX_VALUE);

      ByteBuf m1 = allocator.buffer();
      m1.writeCharSequence("m1", CharsetUtil.UTF_8);
      ByteBuf d1 = allocator.buffer();
      d1.writeCharSequence("d1", CharsetUtil.UTF_8);
      Payload np1 = ByteBufPayload.create(d1, m1);

      ByteBuf m2 = allocator.buffer();
      m2.writeCharSequence("m2", CharsetUtil.UTF_8);
      ByteBuf d2 = allocator.buffer();
      d2.writeCharSequence("d2", CharsetUtil.UTF_8);
      Payload np2 = ByteBufPayload.create(d2, m2);

      ByteBuf m3 = allocator.buffer();
      m3.writeCharSequence("m3", CharsetUtil.UTF_8);
      ByteBuf d3 = allocator.buffer();
      d3.writeCharSequence("d3", CharsetUtil.UTF_8);
      Payload np3 = ByteBufPayload.create(d3, m3);

      FluxSink<Payload> sink = sinks[0];
      RaceTestUtils.race(
          () ->
              RaceTestUtils.race(
                  () -> rule.connection.addToReceivedBuffer(requestNFrame),
                  () -> rule.connection.addToReceivedBuffer(nextFrame1, nextFrame2, nextFrame3),
                  parallel),
          () -> {
            sink.next(np1);
            sink.next(np2);
            sink.next(np3);
            sink.error(new RuntimeException());
          },
          parallel);

      Assertions.assertThat(rule.connection.getSent()).allMatch(ReferenceCounted::release);

      assertSubscriber
          .assertTerminated()
          .assertError(CancellationException.class)
          .assertErrorMessage("Outbound has terminated with an error");
      Assertions.assertThat(assertSubscriber.values())
          .allMatch(
              msg -> {
                ReferenceCountUtil.safeRelease(msg);
                return msg.refCnt() == 0;
              });
      rule.assertHasNoLeaks();
    }
  }

  @Test
  public void checkNoLeaksOnRacingBetweenDownstreamCancelAndOnNextFromRequestStreamTest1() {
    Scheduler parallel = Schedulers.parallel();
    Hooks.onErrorDropped((e) -> {});
    ByteBufAllocator allocator = rule.alloc();
    for (int i = 0; i < 10000; i++) {
      FluxSink<Payload>[] sinks = new FluxSink[1];

      rule.setAcceptingSocket(
          new RSocket() {
            @Override
            public Flux<Payload> requestStream(Payload payload) {
              payload.release();
              return Flux.create(sink -> sinks[0] = sink, FluxSink.OverflowStrategy.IGNORE);
            }
          },
          Integer.MAX_VALUE);

      rule.sendRequest(1, REQUEST_STREAM);

      ByteBuf cancelFrame = CancelFrameCodec.encode(allocator, 1);
      FluxSink<Payload> sink = sinks[0];
      RaceTestUtils.race(
          () -> rule.connection.addToReceivedBuffer(cancelFrame),
          () -> {
            sink.next(ByteBufPayload.create("d1", "m1"));
            sink.next(ByteBufPayload.create("d2", "m2"));
            sink.next(ByteBufPayload.create("d3", "m3"));
          },
          parallel);

      Assertions.assertThat(rule.connection.getSent()).allMatch(ReferenceCounted::release);

      rule.assertHasNoLeaks();
    }
  }

  @Test
  public void checkNoLeaksOnRacingBetweenDownstreamCancelAndOnNextFromRequestResponseTest1() {
    Scheduler parallel = Schedulers.parallel();
    Hooks.onErrorDropped((e) -> {});
    ByteBufAllocator allocator = rule.alloc();
    for (int i = 0; i < 10000; i++) {
      Operators.MonoSubscriber<Payload, Payload>[] sources = new Operators.MonoSubscriber[1];

      rule.setAcceptingSocket(
          new RSocket() {
            @Override
            public Mono<Payload> requestResponse(Payload payload) {
              payload.release();
              return new Mono<Payload>() {
                @Override
                public void subscribe(CoreSubscriber<? super Payload> actual) {
                  sources[0] = new Operators.MonoSubscriber<>(actual);
                  actual.onSubscribe(sources[0]);
                }
              };
            }
          },
          Integer.MAX_VALUE);

      rule.sendRequest(1, REQUEST_RESPONSE);

      ByteBuf cancelFrame = CancelFrameCodec.encode(allocator, 1);
      RaceTestUtils.race(
          () -> rule.connection.addToReceivedBuffer(cancelFrame),
          () -> {
            sources[0].complete(ByteBufPayload.create("d1", "m1"));
          },
          parallel);

      Assertions.assertThat(rule.connection.getSent()).allMatch(ReferenceCounted::release);

      rule.assertHasNoLeaks();
    }
  }

  @Test
  public void simpleDiscardRequestStreamTest() {
    ByteBufAllocator allocator = rule.alloc();
    FluxSink<Payload>[] sinks = new FluxSink[1];

    rule.setAcceptingSocket(
        new RSocket() {
          @Override
          public Flux<Payload> requestStream(Payload payload) {
            payload.release();
            return Flux.create(sink -> sinks[0] = sink, FluxSink.OverflowStrategy.IGNORE);
          }
        },
        1);

    rule.sendRequest(1, REQUEST_STREAM);

    ByteBuf cancelFrame = CancelFrameCodec.encode(allocator, 1);
    FluxSink<Payload> sink = sinks[0];

    sink.next(ByteBufPayload.create("d1", "m1"));
    sink.next(ByteBufPayload.create("d2", "m2"));
    sink.next(ByteBufPayload.create("d3", "m3"));
    rule.connection.addToReceivedBuffer(cancelFrame);

    Assertions.assertThat(rule.connection.getSent()).allMatch(ReferenceCounted::release);

    rule.assertHasNoLeaks();
  }

  @Test
  public void simpleDiscardRequestChannelTest() {
    ByteBufAllocator allocator = rule.alloc();

    rule.setAcceptingSocket(
        new RSocket() {
          @Override
          public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
            return (Flux<Payload>) payloads;
          }
        },
        1);

    rule.sendRequest(1, REQUEST_STREAM);

    ByteBuf cancelFrame = CancelFrameCodec.encode(allocator, 1);

    ByteBuf metadata1 = allocator.buffer();
    metadata1.writeCharSequence("abc1", CharsetUtil.UTF_8);
    ByteBuf data1 = allocator.buffer();
    data1.writeCharSequence("def1", CharsetUtil.UTF_8);
    ByteBuf nextFrame1 =
        PayloadFrameCodec.encode(allocator, 1, false, false, true, metadata1, data1);

    ByteBuf metadata2 = allocator.buffer();
    metadata2.writeCharSequence("abc2", CharsetUtil.UTF_8);
    ByteBuf data2 = allocator.buffer();
    data2.writeCharSequence("def2", CharsetUtil.UTF_8);
    ByteBuf nextFrame2 =
        PayloadFrameCodec.encode(allocator, 1, false, false, true, metadata2, data2);

    ByteBuf metadata3 = allocator.buffer();
    metadata3.writeCharSequence("abc3", CharsetUtil.UTF_8);
    ByteBuf data3 = allocator.buffer();
    data3.writeCharSequence("de3", CharsetUtil.UTF_8);
    ByteBuf nextFrame3 =
        PayloadFrameCodec.encode(allocator, 1, false, false, true, metadata3, data3);
    rule.connection.addToReceivedBuffer(nextFrame1, nextFrame2, nextFrame3);

    rule.connection.addToReceivedBuffer(cancelFrame);

    Assertions.assertThat(rule.connection.getSent()).allMatch(ReferenceCounted::release);

    rule.assertHasNoLeaks();
  }

  @ParameterizedTest
  @MethodSource("encodeDecodePayloadCases")
  public void verifiesThatFrameWithNoMetadataHasDecodedCorrectlyIntoPayload(
      FrameType frameType, int framesCnt, int responsesCnt) {
    ByteBufAllocator allocator = rule.alloc();
    AssertSubscriber<Payload> assertSubscriber = AssertSubscriber.create(framesCnt);
    TestPublisher<Payload> testPublisher = TestPublisher.create();

    rule.setAcceptingSocket(
        new RSocket() {
          @Override
          public Mono<Void> fireAndForget(Payload payload) {
            Mono.just(payload).subscribe(assertSubscriber);
            return Mono.empty();
          }

          @Override
          public Mono<Payload> requestResponse(Payload payload) {
            Mono.just(payload).subscribe(assertSubscriber);
            return testPublisher.mono();
          }

          @Override
          public Flux<Payload> requestStream(Payload payload) {
            Mono.just(payload).subscribe(assertSubscriber);
            return testPublisher.flux();
          }

          @Override
          public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
            payloads.subscribe(assertSubscriber);
            return testPublisher.flux();
          }
        },
        1);

    rule.sendRequest(1, frameType, ByteBufPayload.create("d"));

    // if responses number is bigger than 1 we have to send one extra requestN
    if (responsesCnt > 1) {
      rule.connection.addToReceivedBuffer(
          RequestNFrameCodec.encode(allocator, 1, responsesCnt - 1));
    }

    // respond with specific number of elements
    for (int i = 0; i < responsesCnt; i++) {
      testPublisher.next(ByteBufPayload.create("rd" + i));
    }

    // Listen to incoming frames. Valid for RequestChannel case only
    if (framesCnt > 1) {
      for (int i = 1; i < responsesCnt; i++) {
        rule.connection.addToReceivedBuffer(
            PayloadFrameCodec.encode(
                allocator,
                1,
                false,
                false,
                true,
                null,
                Unpooled.wrappedBuffer(("d" + (i + 1)).getBytes())));
      }
    }

    if (responsesCnt > 0) {
      Assertions.assertThat(
              rule.connection.getSent().stream().filter(bb -> frameType(bb) != REQUEST_N))
          .describedAs(
              "Interaction Type :[%s]. Expected to observe %s frames sent", frameType, responsesCnt)
          .hasSize(responsesCnt)
          .allMatch(bb -> !FrameHeaderCodec.hasMetadata(bb));
    }

    if (framesCnt > 1) {
      Assertions.assertThat(
              rule.connection.getSent().stream().filter(bb -> frameType(bb) == REQUEST_N))
          .describedAs(
              "Interaction Type :[%s]. Expected to observe single RequestN(%s) frame",
              frameType, framesCnt - 1)
          .hasSize(1)
          .first()
          .matches(bb -> RequestNFrameCodec.requestN(bb) == (framesCnt - 1));
    }

    Assertions.assertThat(rule.connection.getSent()).allMatch(ReferenceCounted::release);

    Assertions.assertThat(assertSubscriber.awaitAndAssertNextValueCount(framesCnt).values())
        .hasSize(framesCnt)
        .allMatch(p -> !p.hasMetadata())
        .allMatch(ReferenceCounted::release);

    rule.assertHasNoLeaks();
  }

  static Stream<Arguments> encodeDecodePayloadCases() {
    return Stream.of(
        Arguments.of(REQUEST_FNF, 1, 0),
        Arguments.of(REQUEST_RESPONSE, 1, 1),
        Arguments.of(REQUEST_STREAM, 1, 5),
        Arguments.of(REQUEST_CHANNEL, 5, 5));
  }

  @ParameterizedTest
  @MethodSource("refCntCases")
  public void ensureSendsErrorOnIllegalRefCntPayload(FrameType frameType) {
    rule.setAcceptingSocket(
        new RSocket() {
          @Override
          public Mono<Payload> requestResponse(Payload payload) {
            Payload invalidPayload = ByteBufPayload.create("test", "test");
            invalidPayload.release();
            return Mono.just(invalidPayload);
          }

          @Override
          public Flux<Payload> requestStream(Payload payload) {
            Payload invalidPayload = ByteBufPayload.create("test", "test");
            invalidPayload.release();
            return Flux.just(invalidPayload);
          }

          @Override
          public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
            Payload invalidPayload = ByteBufPayload.create("test", "test");
            invalidPayload.release();
            return Flux.just(invalidPayload);
          }
        });

    rule.sendRequest(1, frameType);

    Assertions.assertThat(rule.connection.getSent())
        .hasSize(1)
        .first()
        .matches(
            bb -> frameType(bb) == ERROR,
            "Expect frame type to be {"
                + ERROR
                + "} but was {"
                + frameType(rule.connection.getSent().iterator().next())
                + "}");
  }

  private static Stream<FrameType> refCntCases() {
    return Stream.of(REQUEST_RESPONSE, REQUEST_STREAM, REQUEST_CHANNEL);
  }

  @Test
  @Disabled("Reactor 3.4.0 should fix that. No need to do anything on our side")
  // see https://github.com/rsocket/rsocket-java/issues/858
  public void testWorkaround858() {
    ByteBuf buffer = rule.alloc().buffer();
    buffer.writeCharSequence("test", CharsetUtil.UTF_8);

    TestPublisher<Payload> testPublisher = TestPublisher.create();

    rule.setAcceptingSocket(
        new RSocket() {
          @Override
          public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
            Flux.from(payloads).doOnNext(ReferenceCounted::release).subscribe();

            return testPublisher.flux();
          }
        });

    rule.connection.addToReceivedBuffer(
        RequestChannelFrameCodec.encodeReleasingPayload(
            rule.alloc(), 1, false, 1, ByteBufPayload.create(buffer)));
    rule.connection.addToReceivedBuffer(
        ErrorFrameCodec.encode(rule.alloc(), 1, new RuntimeException("test")));

    Assertions.assertThat(rule.connection.getSent())
        .hasSize(1)
        .first()
        .matches(bb -> FrameHeaderCodec.frameType(bb) == REQUEST_N)
        .matches(ReferenceCounted::release);

    Assertions.assertThat(rule.socket.isDisposed()).isFalse();
    testPublisher.assertWasCancelled();

    rule.assertHasNoLeaks();
  }

  static Stream<FrameType> requestCases() {
    return Stream.of(REQUEST_FNF, REQUEST_RESPONSE, REQUEST_STREAM, REQUEST_CHANNEL);
  }

  @DisplayName("reassembles payload")
  @ParameterizedTest
  @MethodSource("requestCases")
  void reassemblePayload(FrameType frameType) {
    AtomicReference<Payload> receivedPayload = new AtomicReference<>();
    rule.setAcceptingSocket(
        new RSocket() {
          @Override
          public Mono<Void> fireAndForget(Payload payload) {
            receivedPayload.set(payload);
            return Mono.empty();
          }

          @Override
          public Mono<Payload> requestResponse(Payload payload) {
            receivedPayload.set(payload);
            return Mono.just(genericPayload(rule.allocator));
          }

          @Override
          public Flux<Payload> requestStream(Payload payload) {
            receivedPayload.set(payload);
            return Flux.just(genericPayload(rule.allocator));
          }

          @Override
          public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
            Flux.from(payloads).subscribe(receivedPayload::set, null, null, s -> s.request(1));
            return Flux.just(genericPayload(rule.allocator));
          }
        });

    final int mtu = ThreadLocalRandom.current().nextInt(64, 256);
    final Payload randomPayload = randomPayload(rule.allocator);
    List<ByteBuf> fragments = prepareFragments(rule.allocator, mtu, randomPayload, frameType);

    rule.connection.addToReceivedBuffer(fragments.toArray(new ByteBuf[0]));

    PayloadAssert.assertThat(receivedPayload.get()).isEqualTo(randomPayload).hasNoLeaks();
    randomPayload.release();

    if (frameType != REQUEST_FNF) {
      FrameAssert.assertThat(rule.connection.getSent().poll())
          .typeOf(frameType == REQUEST_RESPONSE ? NEXT_COMPLETE : NEXT)
          .hasData(TestRequesterResponderSupport.DATA_CONTENT)
          .hasMetadata(TestRequesterResponderSupport.METADATA_CONTENT)
          .hasNoLeaks();
      if (frameType != REQUEST_RESPONSE) {
        FrameAssert.assertThat(rule.connection.getSent().poll()).typeOf(COMPLETE).hasNoLeaks();
      }
    }

    rule.assertHasNoLeaks();
  }

  @DisplayName("reassembles metadata")
  @ParameterizedTest
  @MethodSource("requestCases")
  void reassembleMetadataOnly(FrameType frameType) {
    AtomicReference<Payload> receivedPayload = new AtomicReference<>();
    rule.setAcceptingSocket(
        new RSocket() {
          @Override
          public Mono<Void> fireAndForget(Payload payload) {
            receivedPayload.set(payload);
            return Mono.empty();
          }

          @Override
          public Mono<Payload> requestResponse(Payload payload) {
            receivedPayload.set(payload);
            return Mono.just(genericPayload(rule.allocator));
          }

          @Override
          public Flux<Payload> requestStream(Payload payload) {
            receivedPayload.set(payload);
            return Flux.just(genericPayload(rule.allocator));
          }

          @Override
          public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
            Flux.from(payloads).subscribe(receivedPayload::set, null, null, s -> s.request(1));
            return Flux.just(genericPayload(rule.allocator));
          }
        });

    final int mtu = ThreadLocalRandom.current().nextInt(64, 256);
    final Payload randomMetadataOnlyPayload = randomMetadataOnlyPayload(rule.allocator);
    List<ByteBuf> fragments =
        prepareFragments(rule.allocator, mtu, randomMetadataOnlyPayload, frameType);

    rule.connection.addToReceivedBuffer(fragments.toArray(new ByteBuf[0]));

    PayloadAssert.assertThat(receivedPayload.get())
        .isEqualTo(randomMetadataOnlyPayload)
        .hasNoLeaks();
    randomMetadataOnlyPayload.release();

    if (frameType != REQUEST_FNF) {
      FrameAssert.assertThat(rule.connection.getSent().poll())
          .typeOf(frameType == REQUEST_RESPONSE ? NEXT_COMPLETE : NEXT)
          .hasData(TestRequesterResponderSupport.DATA_CONTENT)
          .hasMetadata(TestRequesterResponderSupport.METADATA_CONTENT)
          .hasNoLeaks();
      if (frameType != REQUEST_RESPONSE) {
        FrameAssert.assertThat(rule.connection.getSent().poll()).typeOf(COMPLETE).hasNoLeaks();
      }
    }

    rule.assertHasNoLeaks();
  }

  @ParameterizedTest(name = "throws error if reassembling payload size exceeds {0}")
  @MethodSource("requestCases")
  public void errorTooBigPayload(FrameType frameType) {
    final int mtu = ThreadLocalRandom.current().nextInt(64, 256);
    final int maxInboundPayloadSize = ThreadLocalRandom.current().nextInt(mtu + 1, 4096);
    AtomicReference<Payload> receivedPayload = new AtomicReference<>();
    rule.setMaxInboundPayloadSize(maxInboundPayloadSize);
    rule.setAcceptingSocket(
        new RSocket() {
          @Override
          public Mono<Void> fireAndForget(Payload payload) {
            receivedPayload.set(payload);
            return Mono.empty();
          }

          @Override
          public Mono<Payload> requestResponse(Payload payload) {
            receivedPayload.set(payload);
            return Mono.just(genericPayload(rule.allocator));
          }

          @Override
          public Flux<Payload> requestStream(Payload payload) {
            receivedPayload.set(payload);
            return Flux.just(genericPayload(rule.allocator));
          }

          @Override
          public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
            Flux.from(payloads).subscribe(receivedPayload::set, null, null, s -> s.request(1));
            return Flux.just(genericPayload(rule.allocator));
          }
        });
    final Payload randomPayload = fixedSizePayload(rule.allocator, maxInboundPayloadSize + 1);
    List<ByteBuf> fragments = prepareFragments(rule.allocator, mtu, randomPayload, frameType);
    randomPayload.release();

    rule.connection.addToReceivedBuffer(fragments.toArray(new ByteBuf[0]));

    PayloadAssert.assertThat(receivedPayload.get()).isNull();

    if (frameType != REQUEST_FNF) {
      FrameAssert.assertThat(rule.connection.getSent().poll())
          .typeOf(ERROR)
          .hasData(
              "Failed to reassemble payload. Cause: "
                  + String.format(ILLEGAL_REASSEMBLED_PAYLOAD_SIZE, maxInboundPayloadSize))
          .hasNoLeaks();
    }

    rule.assertHasNoLeaks();
  }

  @ParameterizedTest(name = "throws error if fragment before the last is < min MTU {0}")
  @MethodSource("requestCases")
  public void errorFragmentTooSmall(FrameType frameType) {
    final int mtu = 32;
    AtomicReference<Payload> receivedPayload = new AtomicReference<>();
    rule.setAcceptingSocket(
        new RSocket() {
          @Override
          public Mono<Void> fireAndForget(Payload payload) {
            receivedPayload.set(payload);
            return Mono.empty();
          }

          @Override
          public Mono<Payload> requestResponse(Payload payload) {
            receivedPayload.set(payload);
            return Mono.just(genericPayload(rule.allocator));
          }

          @Override
          public Flux<Payload> requestStream(Payload payload) {
            receivedPayload.set(payload);
            return Flux.just(genericPayload(rule.allocator));
          }

          @Override
          public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
            Flux.from(payloads).subscribe(receivedPayload::set, null, null, s -> s.request(1));
            return Flux.just(genericPayload(rule.allocator));
          }
        });
    final Payload randomPayload = fixedSizePayload(rule.allocator, 156);
    List<ByteBuf> fragments = prepareFragments(rule.allocator, mtu, randomPayload, frameType);
    randomPayload.release();

    rule.connection.addToReceivedBuffer(fragments.toArray(new ByteBuf[0]));

    PayloadAssert.assertThat(receivedPayload.get()).isNull();

    if (frameType != REQUEST_FNF) {
      FrameAssert.assertThat(rule.connection.getSent().poll())
          .typeOf(ERROR)
          .hasData("Failed to reassemble payload. Cause: Fragment is too small.")
          .hasNoLeaks();
    }

    rule.assertHasNoLeaks();
  }

  @ParameterizedTest
  @MethodSource("requestCases")
  void receivingRequestOnStreamIdThaIsAlreadyInUseMUSTBeIgnored_ReassemblyCase(
      FrameType requestType) {
    AtomicReference<Payload> receivedPayload = new AtomicReference<>();
    final MonoProcessor<Void> delayer = MonoProcessor.create();
    rule.setAcceptingSocket(
        new RSocket() {

          @Override
          public Mono<Void> fireAndForget(Payload payload) {
            receivedPayload.set(payload);
            return delayer;
          }

          @Override
          public Mono<Payload> requestResponse(Payload payload) {
            receivedPayload.set(payload);
            return Mono.just(genericPayload(rule.allocator)).delaySubscription(delayer);
          }

          @Override
          public Flux<Payload> requestStream(Payload payload) {
            receivedPayload.set(payload);
            return Flux.just(genericPayload(rule.allocator)).delaySubscription(delayer);
          }

          @Override
          public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
            Flux.from(payloads).subscribe(receivedPayload::set, null, null, s -> s.request(1));
            return Flux.just(genericPayload(rule.allocator)).delaySubscription(delayer);
          }
        });
    final Payload randomPayload1 = fixedSizePayload(rule.allocator, 128);
    final List<ByteBuf> fragments1 =
        prepareFragments(rule.allocator, 64, randomPayload1, requestType);
    final Payload randomPayload2 = fixedSizePayload(rule.allocator, 128);
    final List<ByteBuf> fragments2 =
        prepareFragments(rule.allocator, 64, randomPayload2, requestType);
    randomPayload2.release();
    rule.connection.addToReceivedBuffer(fragments1.remove(0));
    rule.connection.addToReceivedBuffer(fragments2.remove(0));

    rule.connection.addToReceivedBuffer(fragments1.toArray(new ByteBuf[0]));
    if (requestType != REQUEST_CHANNEL) {
      rule.connection.addToReceivedBuffer(fragments2.toArray(new ByteBuf[0]));
      delayer.onComplete();
    } else {
      delayer.onComplete();
      rule.connection.addToReceivedBuffer(PayloadFrameCodec.encodeComplete(rule.allocator, 1));
      rule.connection.addToReceivedBuffer(fragments2.toArray(new ByteBuf[0]));
    }

    PayloadAssert.assertThat(receivedPayload.get()).isEqualTo(randomPayload1).hasNoLeaks();
    randomPayload1.release();

    if (requestType != REQUEST_FNF) {
      FrameAssert.assertThat(rule.connection.getSent().poll())
          .typeOf(requestType == REQUEST_RESPONSE ? NEXT_COMPLETE : NEXT)
          .hasNoLeaks();

      if (requestType != REQUEST_RESPONSE) {
        FrameAssert.assertThat(rule.connection.getSent().poll()).typeOf(COMPLETE).hasNoLeaks();
      }
    }

    rule.assertHasNoLeaks();
  }

  @ParameterizedTest
  @MethodSource("requestCases")
  void receivingRequestOnStreamIdThaIsAlreadyInUseMUSTBeIgnored(FrameType requestType) {
    Assumptions.assumeThat(requestType).isNotEqualTo(REQUEST_FNF);
    AtomicReference<Payload> receivedPayload = new AtomicReference<>();
    final MonoProcessor<Object> delayer = MonoProcessor.create();
    rule.setAcceptingSocket(
        new RSocket() {
          @Override
          public Mono<Payload> requestResponse(Payload payload) {
            receivedPayload.set(payload);
            return Mono.just(genericPayload(rule.allocator)).delaySubscription(delayer);
          }

          @Override
          public Flux<Payload> requestStream(Payload payload) {
            receivedPayload.set(payload);
            return Flux.just(genericPayload(rule.allocator)).delaySubscription(delayer);
          }

          @Override
          public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
            Flux.from(payloads).subscribe(receivedPayload::set, null, null, s -> s.request(1));
            return Flux.just(genericPayload(rule.allocator)).delaySubscription(delayer);
          }
        });
    final Payload randomPayload1 = fixedSizePayload(rule.allocator, 64);
    final Payload randomPayload2 = fixedSizePayload(rule.allocator, 64);
    rule.sendRequest(1, requestType, randomPayload1.retain());
    rule.sendRequest(1, requestType, randomPayload2);

    delayer.onComplete();

    PayloadAssert.assertThat(receivedPayload.get()).isEqualTo(randomPayload1).hasNoLeaks();
    randomPayload1.release();

    FrameAssert.assertThat(rule.connection.getSent().poll())
        .typeOf(requestType == REQUEST_RESPONSE ? NEXT_COMPLETE : NEXT)
        .hasNoLeaks();

    if (requestType != REQUEST_RESPONSE) {
      FrameAssert.assertThat(rule.connection.getSent().poll()).typeOf(COMPLETE).hasNoLeaks();
    }

    rule.assertHasNoLeaks();
  }

  public static class ServerSocketRule extends AbstractSocketRule<RSocketResponder> {

    private RSocket acceptingSocket;
    private volatile int prefetch;

    @Override
    protected void init() {
      acceptingSocket =
          new RSocket() {
            @Override
            public Mono<Payload> requestResponse(Payload payload) {
              return Mono.just(payload);
            }
          };
      super.init();
    }

    public void setAcceptingSocket(RSocket acceptingSocket) {
      this.acceptingSocket = acceptingSocket;
      connection = new TestDuplexConnection(alloc());
      connectSub = TestSubscriber.create();
      this.prefetch = Integer.MAX_VALUE;
      super.init();
    }

    public void setAcceptingSocket(RSocket acceptingSocket, int prefetch) {
      this.acceptingSocket = acceptingSocket;
      connection = new TestDuplexConnection(alloc());
      connectSub = TestSubscriber.create();
      this.prefetch = prefetch;
      super.init();
    }

    @Override
    protected RSocketResponder newRSocket() {
      return new RSocketResponder(
          connection,
          acceptingSocket,
          PayloadDecoder.ZERO_COPY,
          ResponderLeaseHandler.None,
          0,
          maxFrameLength,
          maxInboundPayloadSize);
    }

    private void sendRequest(int streamId, FrameType frameType) {
      sendRequest(streamId, frameType, EmptyPayload.INSTANCE);
    }

    private void sendRequest(int streamId, FrameType frameType, Payload payload) {
      ByteBuf request;

      switch (frameType) {
        case REQUEST_CHANNEL:
          request =
              RequestChannelFrameCodec.encodeReleasingPayload(
                  allocator, streamId, false, prefetch, payload);
          break;
        case REQUEST_STREAM:
          request =
              RequestStreamFrameCodec.encodeReleasingPayload(
                  allocator, streamId, prefetch, payload);
          break;
        case REQUEST_RESPONSE:
          request = RequestResponseFrameCodec.encodeReleasingPayload(allocator, streamId, payload);
          break;
        case REQUEST_FNF:
          request =
              RequestFireAndForgetFrameCodec.encodeReleasingPayload(allocator, streamId, payload);
          break;
        default:
          throw new IllegalArgumentException("unsupported type: " + frameType);
      }

      connection.addToReceivedBuffer(request);
    }
  }
}
