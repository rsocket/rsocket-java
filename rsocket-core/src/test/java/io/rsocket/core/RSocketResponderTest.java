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
import static io.rsocket.frame.FrameHeaderFlyweight.frameType;
import static io.rsocket.frame.FrameType.REQUEST_CHANNEL;
import static io.rsocket.frame.FrameType.REQUEST_RESPONSE;
import static io.rsocket.frame.FrameType.REQUEST_STREAM;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
import io.rsocket.frame.CancelFrameFlyweight;
import io.rsocket.frame.ErrorFrameFlyweight;
import io.rsocket.frame.FrameHeaderFlyweight;
import io.rsocket.frame.FrameLengthFlyweight;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.KeepAliveFrameFlyweight;
import io.rsocket.frame.PayloadFrameFlyweight;
import io.rsocket.frame.RequestChannelFrameFlyweight;
import io.rsocket.frame.RequestNFrameFlyweight;
import io.rsocket.frame.RequestResponseFrameFlyweight;
import io.rsocket.frame.RequestStreamFrameFlyweight;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.subscriber.AssertSubscriber;
import io.rsocket.lease.ResponderLeaseHandler;
import io.rsocket.test.util.TestDuplexConnection;
import io.rsocket.test.util.TestSubscriber;
import io.rsocket.util.ByteBufPayload;
import io.rsocket.util.DefaultPayload;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.runners.model.Statement;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.util.RaceTestUtils;

public class RSocketResponderTest {

  ServerSocketRule rule;

  @BeforeEach
  public void setUp() throws Throwable {
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
  }

  @Test
  @Timeout(2_000)
  @Disabled
  public void testHandleKeepAlive() throws Exception {
    rule.connection.addToReceivedBuffer(
        KeepAliveFrameFlyweight.encode(rule.alloc(), true, 0, Unpooled.EMPTY_BUFFER));
    ByteBuf sent = rule.connection.awaitSend();
    assertThat("Unexpected frame sent.", frameType(sent), is(FrameType.KEEPALIVE));
    /*Keep alive ack must not have respond flag else, it will result in infinite ping-pong of keep alive frames.*/
    assertThat(
        "Unexpected keep-alive frame respond flag.",
        KeepAliveFrameFlyweight.respondFlag(sent),
        is(false));
  }

  @Test
  @Timeout(2_000)
  @Disabled
  public void testHandleResponseFrameNoError() throws Exception {
    final int streamId = 4;
    rule.connection.clearSendReceiveBuffers();

    rule.sendRequest(streamId, FrameType.REQUEST_RESPONSE);

    Collection<Subscriber<ByteBuf>> sendSubscribers = rule.connection.getSendSubscribers();
    assertThat("Request not sent.", sendSubscribers, hasSize(1));
    assertThat("Unexpected error.", rule.errors, is(empty()));
    Subscriber<ByteBuf> sendSub = sendSubscribers.iterator().next();
    assertThat(
        "Unexpected frame sent.",
        frameType(rule.connection.awaitSend()),
        anyOf(is(FrameType.COMPLETE), is(FrameType.NEXT_COMPLETE)));
  }

  @Test
  @Timeout(2_000)
  @Disabled
  public void testHandlerEmitsError() throws Exception {
    final int streamId = 4;
    rule.sendRequest(streamId, FrameType.REQUEST_STREAM);
    assertThat("Unexpected error.", rule.errors, is(empty()));
    assertThat(
        "Unexpected frame sent.", frameType(rule.connection.awaitSend()), is(FrameType.ERROR));
  }

  @Test
  @Timeout(20_000)
  public void testCancel() {
    ByteBufAllocator allocator = rule.alloc();
    final int streamId = 4;
    final AtomicBoolean cancelled = new AtomicBoolean();
    rule.setAcceptingSocket(
        new AbstractRSocket() {
          @Override
          public Mono<Payload> requestResponse(Payload payload) {
            payload.release();
            return Mono.<Payload>never().doOnCancel(() -> cancelled.set(true));
          }
        });
    rule.sendRequest(streamId, FrameType.REQUEST_RESPONSE);

    assertThat("Unexpected error.", rule.errors, is(empty()));
    assertThat("Unexpected frame sent.", rule.connection.getSent(), is(empty()));

    rule.connection.addToReceivedBuffer(CancelFrameFlyweight.encode(allocator, streamId));

    assertThat("Unexpected frame sent.", rule.connection.getSent(), is(empty()));
    assertThat("Subscription not cancelled.", cancelled.get(), is(true));
    rule.assertHasNoLeaks();
  }

  @Test
  @Timeout(2_000)
  public void shouldThrownExceptionIfGivenPayloadIsExitsSizeAllowanceWithNoFragmentation() {
    final int streamId = 4;
    final AtomicBoolean cancelled = new AtomicBoolean();
    byte[] metadata = new byte[FrameLengthFlyweight.FRAME_LENGTH_MASK];
    byte[] data = new byte[FrameLengthFlyweight.FRAME_LENGTH_MASK];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);
    final Payload payload = DefaultPayload.create(data, metadata);
    final AbstractRSocket acceptingSocket =
        new AbstractRSocket() {
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
          // FIXME
          //          @Override
          //          public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
          //            Flux.from(payloads)
          //                .doOnNext(Payload::release)
          //                .subscribe(
          //                    new BaseSubscriber<Payload>() {
          //                      @Override
          //                      protected void hookOnSubscribe(Subscription subscription) {
          //                        subscription.request(1);
          //                      }
          //                    });
          //            return Flux.just(payload).doOnCancel(() -> cancelled.set(true));
          //          }
        };
    rule.setAcceptingSocket(acceptingSocket);

    final Runnable[] runnables = {
      () -> rule.sendRequest(streamId, FrameType.REQUEST_RESPONSE),
      () -> rule.sendRequest(streamId, FrameType.REQUEST_STREAM) /* FIXME,
      () -> rule.sendRequest(streamId, FrameType.REQUEST_CHANNEL)*/
    };

    for (Runnable runnable : runnables) {
      rule.connection.clearSendReceiveBuffers();
      runnable.run();
      Assertions.assertThat(rule.errors)
          .first()
          .isInstanceOf(IllegalArgumentException.class)
          .hasToString("java.lang.IllegalArgumentException: " + INVALID_PAYLOAD_ERROR_MESSAGE);
      Assertions.assertThat(rule.connection.getSent())
          .filteredOn(bb -> FrameHeaderFlyweight.frameType(bb) == FrameType.ERROR)
          .hasSize(1)
          .first()
          .matches(bb -> ErrorFrameFlyweight.dataUtf8(bb).contains(INVALID_PAYLOAD_ERROR_MESSAGE))
          .matches(ReferenceCounted::release);

      assertThat("Subscription not cancelled.", cancelled.get(), is(true));
    }

    rule.assertHasNoLeaks();
  }

  @Test
  @Disabled("Due to https://github.com/reactor/reactor-core/pull/2114")
  public void checkNoLeaksOnRacingCancelFromRequestChannelAndNextFromUpstream() {

    ByteBufAllocator allocator = rule.alloc();
    for (int i = 0; i < 10000; i++) {
      AssertSubscriber<Payload> assertSubscriber = AssertSubscriber.create();

      rule.setAcceptingSocket(
          new AbstractRSocket() {
            @Override
            public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
              ((Flux<Payload>) payloads)
                  .doOnNext(ReferenceCountUtil::safeRelease)
                  .subscribe(assertSubscriber);
              return Flux.never();
            }
          },
          Integer.MAX_VALUE);

      rule.sendRequest(1, REQUEST_CHANNEL);
      ByteBuf metadata1 = allocator.buffer();
      metadata1.writeCharSequence("abc", CharsetUtil.UTF_8);
      ByteBuf data1 = allocator.buffer();
      data1.writeCharSequence("def", CharsetUtil.UTF_8);
      ByteBuf nextFrame1 =
          PayloadFrameFlyweight.encode(allocator, 1, false, false, true, metadata1, data1);

      ByteBuf metadata2 = allocator.buffer();
      metadata2.writeCharSequence("abc", CharsetUtil.UTF_8);
      ByteBuf data2 = allocator.buffer();
      data2.writeCharSequence("def", CharsetUtil.UTF_8);
      ByteBuf nextFrame2 =
          PayloadFrameFlyweight.encode(allocator, 1, false, false, true, metadata2, data2);

      ByteBuf metadata3 = allocator.buffer();
      metadata3.writeCharSequence("abc", CharsetUtil.UTF_8);
      ByteBuf data3 = allocator.buffer();
      data3.writeCharSequence("def", CharsetUtil.UTF_8);
      ByteBuf nextFrame3 =
          PayloadFrameFlyweight.encode(allocator, 1, false, false, true, metadata3, data3);

      RaceTestUtils.race(
          () -> {
            rule.connection.addToReceivedBuffer(nextFrame1, nextFrame2, nextFrame3);
          },
          assertSubscriber::cancel);

      Assertions.assertThat(rule.connection.getSent()).allMatch(ReferenceCounted::release);

      rule.assertHasNoLeaks();
    }
  }

  @Test
  @Disabled("Due to https://github.com/reactor/reactor-core/pull/2114")
  public void checkNoLeaksOnRacingBetweenDownstreamCancelAndOnNextFromRequestChannelTest() {
    Hooks.onErrorDropped((e) -> {});
    ByteBufAllocator allocator = rule.alloc();
    for (int i = 0; i < 10000; i++) {
      AssertSubscriber<Payload> assertSubscriber = AssertSubscriber.create();

      FluxSink<Payload>[] sinks = new FluxSink[1];

      rule.setAcceptingSocket(
          new AbstractRSocket() {
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

      ByteBuf cancelFrame = CancelFrameFlyweight.encode(allocator, 1);
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
  @Disabled("Due to https://github.com/reactor/reactor-core/pull/2114")
  public void checkNoLeaksOnRacingBetweenDownstreamCancelAndOnNextFromRequestChannelTest1() {
    Scheduler parallel = Schedulers.parallel();
    Hooks.onErrorDropped((e) -> {});
    ByteBufAllocator allocator = rule.alloc();
    for (int i = 0; i < 10000; i++) {
      AssertSubscriber<Payload> assertSubscriber = AssertSubscriber.create();

      FluxSink<Payload>[] sinks = new FluxSink[1];

      rule.setAcceptingSocket(
          new AbstractRSocket() {
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

      ByteBuf cancelFrame = CancelFrameFlyweight.encode(allocator, 1);
      ByteBuf requestNFrame = RequestNFrameFlyweight.encode(allocator, 1, Integer.MAX_VALUE);
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
  @Disabled("Due to https://github.com/reactor/reactor-core/pull/2114")
  public void
      checkNoLeaksOnRacingBetweenDownstreamCancelAndOnNextFromUpstreamOnErrorFromRequestChannelTest1()
          throws InterruptedException {
    Scheduler parallel = Schedulers.parallel();
    Hooks.onErrorDropped((e) -> {});
    ByteBufAllocator allocator = rule.alloc();
    for (int i = 0; i < 10000; i++) {
      FluxSink<Payload>[] sinks = new FluxSink[1];

      rule.setAcceptingSocket(
          new AbstractRSocket() {
            @Override
            public Flux<Payload> requestChannel(Publisher<Payload> payloads) {

              return Flux.<Payload>create(
                      sink -> {
                        sinks[0] = sink;
                      },
                      FluxSink.OverflowStrategy.IGNORE)
                  .mergeWith(payloads);
            }
          },
          1);

      rule.sendRequest(1, REQUEST_CHANNEL);

      ByteBuf metadata1 = allocator.buffer();
      metadata1.writeCharSequence("abc", CharsetUtil.UTF_8);
      ByteBuf data1 = allocator.buffer();
      data1.writeCharSequence("def", CharsetUtil.UTF_8);
      ByteBuf nextFrame1 =
          PayloadFrameFlyweight.encode(allocator, 1, false, false, true, metadata1, data1);

      ByteBuf metadata2 = allocator.buffer();
      metadata2.writeCharSequence("abc", CharsetUtil.UTF_8);
      ByteBuf data2 = allocator.buffer();
      data2.writeCharSequence("def", CharsetUtil.UTF_8);
      ByteBuf nextFrame2 =
          PayloadFrameFlyweight.encode(allocator, 1, false, false, true, metadata2, data2);

      ByteBuf metadata3 = allocator.buffer();
      metadata3.writeCharSequence("abc", CharsetUtil.UTF_8);
      ByteBuf data3 = allocator.buffer();
      data3.writeCharSequence("def", CharsetUtil.UTF_8);
      ByteBuf nextFrame3 =
          PayloadFrameFlyweight.encode(allocator, 1, false, false, true, metadata3, data3);

      ByteBuf requestNFrame = RequestNFrameFlyweight.encode(allocator, 1, Integer.MAX_VALUE);

      FluxSink<Payload> sink = sinks[0];
      RaceTestUtils.race(
          () ->
              RaceTestUtils.race(
                  () -> rule.connection.addToReceivedBuffer(requestNFrame),
                  () -> rule.connection.addToReceivedBuffer(nextFrame1, nextFrame2, nextFrame3),
                  parallel),
          () -> {
            sink.next(ByteBufPayload.create("d1", "m1"));
            sink.next(ByteBufPayload.create("d2", "m2"));
            sink.next(ByteBufPayload.create("d3", "m3"));
            sink.error(new RuntimeException());
          },
          parallel);

      Assertions.assertThat(rule.connection.getSent()).allMatch(ReferenceCounted::release);

      rule.assertHasNoLeaks();
    }
  }

  @Test
  @Disabled("Due to https://github.com/reactor/reactor-core/pull/2114")
  public void checkNoLeaksOnRacingBetweenDownstreamCancelAndOnNextFromRequestStreamTest1() {
    Scheduler parallel = Schedulers.parallel();
    Hooks.onErrorDropped((e) -> {});
    ByteBufAllocator allocator = rule.alloc();
    for (int i = 0; i < 10000; i++) {
      FluxSink<Payload>[] sinks = new FluxSink[1];

      rule.setAcceptingSocket(
          new AbstractRSocket() {
            @Override
            public Flux<Payload> requestStream(Payload payload) {
              payload.release();
              return Flux.create(sink -> sinks[0] = sink, FluxSink.OverflowStrategy.IGNORE);
            }
          },
          Integer.MAX_VALUE);

      rule.sendRequest(1, REQUEST_STREAM);

      ByteBuf cancelFrame = CancelFrameFlyweight.encode(allocator, 1);
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
          new AbstractRSocket() {
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

      ByteBuf cancelFrame = CancelFrameFlyweight.encode(allocator, 1);
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
        new AbstractRSocket() {
          @Override
          public Flux<Payload> requestStream(Payload payload) {
            payload.release();
            return Flux.create(sink -> sinks[0] = sink, FluxSink.OverflowStrategy.IGNORE);
          }
        },
        1);

    rule.sendRequest(1, REQUEST_STREAM);

    ByteBuf cancelFrame = CancelFrameFlyweight.encode(allocator, 1);
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
        new AbstractRSocket() {
          @Override
          public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
            return (Flux<Payload>) payloads;
          }
        },
        1);

    rule.sendRequest(1, REQUEST_STREAM);

    ByteBuf cancelFrame = CancelFrameFlyweight.encode(allocator, 1);

    ByteBuf metadata1 = allocator.buffer();
    metadata1.writeCharSequence("abc", CharsetUtil.UTF_8);
    ByteBuf data1 = allocator.buffer();
    data1.writeCharSequence("def", CharsetUtil.UTF_8);
    ByteBuf nextFrame1 =
        PayloadFrameFlyweight.encode(allocator, 1, false, false, true, metadata1, data1);

    ByteBuf metadata2 = allocator.buffer();
    metadata2.writeCharSequence("abc", CharsetUtil.UTF_8);
    ByteBuf data2 = allocator.buffer();
    data2.writeCharSequence("def", CharsetUtil.UTF_8);
    ByteBuf nextFrame2 =
        PayloadFrameFlyweight.encode(allocator, 1, false, false, true, metadata2, data2);

    ByteBuf metadata3 = allocator.buffer();
    metadata3.writeCharSequence("abc", CharsetUtil.UTF_8);
    ByteBuf data3 = allocator.buffer();
    data3.writeCharSequence("def", CharsetUtil.UTF_8);
    ByteBuf nextFrame3 =
        PayloadFrameFlyweight.encode(allocator, 1, false, false, true, metadata3, data3);
    rule.connection.addToReceivedBuffer(nextFrame1, nextFrame2, nextFrame3);

    rule.connection.addToReceivedBuffer(cancelFrame);

    Assertions.assertThat(rule.connection.getSent()).allMatch(ReferenceCounted::release);

    rule.assertHasNoLeaks();
  }

  public static class ServerSocketRule extends AbstractSocketRule<RSocketResponder> {

    private RSocket acceptingSocket;
    private volatile int prefetch;

    @Override
    protected void init() {
      acceptingSocket =
          new AbstractRSocket() {
            @Override
            public Mono<Payload> requestResponse(Payload payload) {
              return Mono.just(payload);
            }
          };
      super.init();
    }

    public void setAcceptingSocket(RSocket acceptingSocket) {
      this.acceptingSocket = acceptingSocket;
      connection = new TestDuplexConnection();
      connectSub = TestSubscriber.create();
      errors = new ConcurrentLinkedQueue<>();
      this.prefetch = Integer.MAX_VALUE;
      super.init();
    }

    public void setAcceptingSocket(RSocket acceptingSocket, int prefetch) {
      this.acceptingSocket = acceptingSocket;
      connection = new TestDuplexConnection();
      connectSub = TestSubscriber.create();
      errors = new ConcurrentLinkedQueue<>();
      this.prefetch = prefetch;
      super.init();
    }

    @Override
    protected RSocketResponder newRSocket(LeaksTrackingByteBufAllocator allocator) {
      return new RSocketResponder(
          allocator,
          connection,
          acceptingSocket,
          PayloadDecoder.ZERO_COPY,
          throwable -> errors.add(throwable),
          ResponderLeaseHandler.None,
          0);
    }

    private void sendRequest(int streamId, FrameType frameType) {
      ByteBuf request;

      switch (frameType) {
        case REQUEST_CHANNEL:
          request =
              RequestChannelFrameFlyweight.encode(
                  allocator,
                  streamId,
                  false,
                  false,
                  prefetch,
                  Unpooled.EMPTY_BUFFER,
                  Unpooled.EMPTY_BUFFER);
          break;
        case REQUEST_STREAM:
          request =
              RequestStreamFrameFlyweight.encode(
                  allocator,
                  streamId,
                  false,
                  prefetch,
                  Unpooled.EMPTY_BUFFER,
                  Unpooled.EMPTY_BUFFER);
          break;
        case REQUEST_RESPONSE:
          request =
              RequestResponseFrameFlyweight.encode(
                  allocator, streamId, false, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER);
          break;
        default:
          throw new IllegalArgumentException("unsupported type: " + frameType);
      }

      connection.addToReceivedBuffer(request);
    }
  }
}
