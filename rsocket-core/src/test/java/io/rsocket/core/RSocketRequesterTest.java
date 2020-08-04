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
import static io.rsocket.frame.FrameType.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.rsocket.FrameAssert;
import io.rsocket.Payload;
import io.rsocket.PayloadAssert;
import io.rsocket.RSocket;
import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
import io.rsocket.exceptions.ApplicationErrorException;
import io.rsocket.exceptions.CustomRSocketException;
import io.rsocket.exceptions.RejectedSetupException;
import io.rsocket.frame.CancelFrameCodec;
import io.rsocket.frame.ErrorFrameCodec;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameLengthCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.PayloadFrameCodec;
import io.rsocket.frame.RequestChannelFrameCodec;
import io.rsocket.frame.RequestFireAndForgetFrameCodec;
import io.rsocket.frame.RequestNFrameCodec;
import io.rsocket.frame.RequestResponseFrameCodec;
import io.rsocket.frame.RequestStreamFrameCodec;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.subscriber.AssertSubscriber;
import io.rsocket.lease.RequesterLeaseHandler;
import io.rsocket.test.util.TestSubscriber;
import io.rsocket.util.ByteBufPayload;
import io.rsocket.util.DefaultPayload;
import io.rsocket.util.EmptyPayload;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
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
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.util.RaceTestUtils;

public class RSocketRequesterTest {

  ClientSocketRule rule;

  @BeforeEach
  public void setUp() throws Throwable {
    Hooks.onNextDropped(ReferenceCountUtil::safeRelease);
    Hooks.onErrorDropped((t) -> {});
    rule = new ClientSocketRule();
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
  public void testInvalidFrameOnStream0ShouldNotTerminateRSocket() {
    rule.connection.addToReceivedBuffer(RequestNFrameCodec.encode(rule.alloc(), 0, 10));
    Assertions.assertThat(rule.socket.isDisposed()).isFalse();
    rule.assertHasNoLeaks();
  }

  @Test
  @Timeout(2_000)
  public void testStreamInitialN() {
    Flux<Payload> stream = rule.socket.requestStream(EmptyPayload.INSTANCE);

    BaseSubscriber<Payload> subscriber =
        new BaseSubscriber<Payload>() {
          @Override
          protected void hookOnSubscribe(Subscription subscription) {
            // don't request here
          }
        };
    stream.subscribe(subscriber);

    Assertions.assertThat(rule.connection.getSent()).isEmpty();

    subscriber.request(5);

    List<ByteBuf> sent = new ArrayList<>(rule.connection.getSent());

    assertThat("sent frame count", sent.size(), is(1));

    ByteBuf f = sent.get(0);

    assertThat("initial frame", frameType(f), is(REQUEST_STREAM));
    assertThat("initial request n", RequestStreamFrameCodec.initialRequestN(f), is(5L));
    assertThat("should be released", f.release(), is(true));
    rule.assertHasNoLeaks();
  }

  @Test
  @Timeout(2_000)
  public void testHandleSetupException() {
    rule.connection.addToReceivedBuffer(
        ErrorFrameCodec.encode(rule.alloc(), 0, new RejectedSetupException("boom")));
    Assertions.assertThatThrownBy(() -> rule.socket.onClose().block())
        .isInstanceOf(RejectedSetupException.class);
    rule.assertHasNoLeaks();
  }

  @Test
  @Timeout(2_000)
  public void testHandleApplicationException() {
    rule.connection.clearSendReceiveBuffers();
    Publisher<Payload> response = rule.socket.requestResponse(EmptyPayload.INSTANCE);
    Subscriber<Payload> responseSub = TestSubscriber.create();
    response.subscribe(responseSub);

    int streamId = rule.getStreamIdForRequestType(REQUEST_RESPONSE);
    rule.connection.addToReceivedBuffer(
        ErrorFrameCodec.encode(rule.alloc(), streamId, new ApplicationErrorException("error")));

    verify(responseSub).onError(any(ApplicationErrorException.class));

    Assertions.assertThat(rule.connection.getSent())
        // requestResponseFrame
        .hasSize(1)
        .allMatch(ReferenceCounted::release);

    rule.assertHasNoLeaks();
  }

  @Test
  @Timeout(2_000)
  public void testHandleValidFrame() {
    Publisher<Payload> response = rule.socket.requestResponse(EmptyPayload.INSTANCE);
    Subscriber<Payload> sub = TestSubscriber.create();
    response.subscribe(sub);

    int streamId = rule.getStreamIdForRequestType(REQUEST_RESPONSE);
    rule.connection.addToReceivedBuffer(
        PayloadFrameCodec.encodeNextReleasingPayload(
            rule.alloc(), streamId, EmptyPayload.INSTANCE));

    verify(sub).onComplete();
    Assertions.assertThat(rule.connection.getSent()).hasSize(1).allMatch(ReferenceCounted::release);
    rule.assertHasNoLeaks();
  }

  @Test
  @Timeout(2_000)
  public void testRequestReplyWithCancel() {
    Mono<Payload> response = rule.socket.requestResponse(EmptyPayload.INSTANCE);

    try {
      response.block(Duration.ofMillis(100));
    } catch (IllegalStateException ise) {
    }

    List<ByteBuf> sent = new ArrayList<>(rule.connection.getSent());

    assertThat(
        "Unexpected frame sent on the connection.", frameType(sent.get(0)), is(REQUEST_RESPONSE));
    assertThat("Unexpected frame sent on the connection.", frameType(sent.get(1)), is(CANCEL));
    Assertions.assertThat(sent).hasSize(2).allMatch(ReferenceCounted::release);
    rule.assertHasNoLeaks();
  }

  @Test
  @Disabled("invalid")
  @Timeout(2_000)
  public void testRequestReplyErrorOnSend() {
    rule.connection.setAvailability(0); // Fails send
    Mono<Payload> response = rule.socket.requestResponse(EmptyPayload.INSTANCE);
    Subscriber<Payload> responseSub = TestSubscriber.create(10);
    response.subscribe(responseSub);

    this.rule
        .socket
        .onClose()
        .as(StepVerifier::create)
        .expectComplete()
        .verify(Duration.ofMillis(100));

    verify(responseSub).onSubscribe(any(Subscription.class));

    rule.assertHasNoLeaks();
    // TODO this should get the error reported through the response subscription
    //    verify(responseSub).onError(any(RuntimeException.class));
  }

  @Test
  @Timeout(2_000)
  public void testChannelRequestCancellation() {
    MonoProcessor<Void> cancelled = MonoProcessor.create();
    Flux<Payload> request = Flux.<Payload>never().doOnCancel(cancelled::onComplete);
    rule.socket.requestChannel(request).subscribe().dispose();
    Flux.first(
            cancelled,
            Flux.error(new IllegalStateException("Channel request not cancelled"))
                .delaySubscription(Duration.ofSeconds(1)))
        .blockFirst();
    rule.assertHasNoLeaks();
  }

  @Test
  @Timeout(2_000)
  public void testChannelRequestCancellation2() {
    MonoProcessor<Void> cancelled = MonoProcessor.create();
    Flux<Payload> request =
        Flux.<Payload>just(EmptyPayload.INSTANCE).repeat(259).doOnCancel(cancelled::onComplete);
    rule.socket.requestChannel(request).subscribe().dispose();
    Flux.first(
            cancelled,
            Flux.error(new IllegalStateException("Channel request not cancelled"))
                .delaySubscription(Duration.ofSeconds(1)))
        .blockFirst();
    Assertions.assertThat(rule.connection.getSent()).allMatch(ReferenceCounted::release);
    rule.assertHasNoLeaks();
  }

  @Test
  public void testChannelRequestServerSideCancellation() {
    MonoProcessor<Payload> cancelled = MonoProcessor.create();
    UnicastProcessor<Payload> request = UnicastProcessor.create();
    request.onNext(EmptyPayload.INSTANCE);
    rule.socket.requestChannel(request).subscribe(cancelled);
    int streamId = rule.getStreamIdForRequestType(REQUEST_CHANNEL);
    rule.connection.addToReceivedBuffer(CancelFrameCodec.encode(rule.alloc(), streamId));
    rule.connection.addToReceivedBuffer(PayloadFrameCodec.encodeComplete(rule.alloc(), streamId));
    Flux.first(
            cancelled,
            Flux.error(new IllegalStateException("Channel request not cancelled"))
                .delaySubscription(Duration.ofSeconds(1)))
        .blockFirst();

    Assertions.assertThat(request.isDisposed()).isTrue();
    Assertions.assertThat(rule.connection.getSent())
        .hasSize(1)
        .first()
        .matches(bb -> frameType(bb) == REQUEST_CHANNEL)
        .matches(ReferenceCounted::release);
    rule.assertHasNoLeaks();
  }

  @Test
  public void testCorrectFrameOrder() {
    MonoProcessor<Object> delayer = MonoProcessor.create();
    BaseSubscriber<Payload> subscriber =
        new BaseSubscriber<Payload>() {
          @Override
          protected void hookOnSubscribe(Subscription subscription) {}
        };
    rule.socket
        .requestChannel(
            Flux.concat(Flux.just(0).delayUntil(i -> delayer), Flux.range(1, 999))
                .map(i -> DefaultPayload.create(i + "")))
        .subscribe(subscriber);

    subscriber.request(1);
    subscriber.request(Long.MAX_VALUE);
    delayer.onComplete();

    Iterator<ByteBuf> iterator = rule.connection.getSent().iterator();

    ByteBuf initialFrame = iterator.next();

    Assertions.assertThat(FrameHeaderCodec.frameType(initialFrame)).isEqualTo(REQUEST_CHANNEL);
    Assertions.assertThat(RequestChannelFrameCodec.initialRequestN(initialFrame))
        .isEqualTo(Long.MAX_VALUE);
    Assertions.assertThat(RequestChannelFrameCodec.data(initialFrame).toString(CharsetUtil.UTF_8))
        .isEqualTo("0");
    Assertions.assertThat(initialFrame.release()).isTrue();

    Assertions.assertThat(iterator.hasNext()).isFalse();
    rule.assertHasNoLeaks();
  }

  @ParameterizedTest
  @ValueSource(ints = {128, 256, FRAME_LENGTH_MASK})
  public void shouldThrownExceptionIfGivenPayloadIsExitsSizeAllowanceWithNoFragmentation(
      int maxFrameLength) {
    rule.setMaxFrameLength(maxFrameLength);
    prepareCalls()
        .forEach(
            generator -> {
              byte[] metadata = new byte[maxFrameLength];
              byte[] data = new byte[maxFrameLength];
              ThreadLocalRandom.current().nextBytes(metadata);
              ThreadLocalRandom.current().nextBytes(data);
              StepVerifier.create(
                      generator.apply(rule.socket, DefaultPayload.create(data, metadata)))
                  .expectSubscription()
                  .expectErrorSatisfies(
                      t ->
                          Assertions.assertThat(t)
                              .isInstanceOf(IllegalArgumentException.class)
                              .hasMessage(
                                  String.format(INVALID_PAYLOAD_ERROR_MESSAGE, maxFrameLength)))
                  .verify();
              rule.assertHasNoLeaks();
            });
  }

  static Stream<BiFunction<RSocket, Payload, Publisher<?>>> prepareCalls() {
    return Stream.of(
        RSocket::fireAndForget,
        RSocket::requestResponse,
        RSocket::requestStream,
        (rSocket, payload) -> rSocket.requestChannel(Flux.just(payload)),
        RSocket::metadataPush);
  }

  @ParameterizedTest
  @ValueSource(ints = {128, 256, FrameLengthCodec.FRAME_LENGTH_MASK})
  public void
      shouldThrownExceptionIfGivenPayloadIsExitsSizeAllowanceWithNoFragmentationForRequestChannelCase(
          int maxFrameLength) {
    rule.setMaxFrameLength(maxFrameLength);
    byte[] metadata = new byte[maxFrameLength];
    byte[] data = new byte[maxFrameLength];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);
    StepVerifier.create(
            rule.socket.requestChannel(
                Flux.just(EmptyPayload.INSTANCE, DefaultPayload.create(data, metadata))),
            0)
        .expectSubscription()
        .thenRequest(2)
        .then(
            () -> {
              rule.connection.addToReceivedBuffer(
                  RequestNFrameCodec.encode(
                      rule.alloc(), rule.getStreamIdForRequestType(REQUEST_CHANNEL), 2));
            })
        .expectErrorSatisfies(
            t ->
                Assertions.assertThat(t)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(String.format(INVALID_PAYLOAD_ERROR_MESSAGE, maxFrameLength)))
        .verify();
    Assertions.assertThat(rule.connection.getSent())
        // expect to be sent RequestChannelFrame
        // expect to be sent CancelFrame
        .hasSize(2)
        .allMatch(ReferenceCounted::release);
    rule.assertHasNoLeaks();
  }

  @ParameterizedTest
  @MethodSource("racingCases")
  public void checkNoLeaksOnRacing(
      Function<ClientSocketRule, Publisher<Payload>> initiator,
      BiConsumer<AssertSubscriber<Payload>, ClientSocketRule> runner) {
    for (int i = 0; i < 10000; i++) {
      ClientSocketRule clientSocketRule = new ClientSocketRule();
      try {
        clientSocketRule
            .apply(
                new Statement() {
                  @Override
                  public void evaluate() {}
                },
                null)
            .evaluate();
      } catch (Throwable throwable) {
        throwable.printStackTrace();
      }

      Publisher<Payload> payloadP = initiator.apply(clientSocketRule);
      AssertSubscriber<Payload> assertSubscriber = AssertSubscriber.create(0);

      if (payloadP instanceof Flux) {
        ((Flux<Payload>) payloadP).doOnNext(Payload::release).subscribe(assertSubscriber);
      } else {
        ((Mono<Payload>) payloadP).doOnNext(Payload::release).subscribe(assertSubscriber);
      }

      runner.accept(assertSubscriber, clientSocketRule);

      Assertions.assertThat(clientSocketRule.connection.getSent())
          .allMatch(ReferenceCounted::release);

      clientSocketRule.assertHasNoLeaks();
    }
  }

  private static Stream<Arguments> racingCases() {
    return Stream.of(
        Arguments.of(
            (Function<ClientSocketRule, Publisher<Payload>>)
                (rule) -> rule.socket.requestStream(EmptyPayload.INSTANCE),
            (BiConsumer<AssertSubscriber<Payload>, ClientSocketRule>)
                (as, rule) -> {
                  ByteBufAllocator allocator = rule.alloc();
                  ByteBuf metadata = allocator.buffer();
                  metadata.writeCharSequence("abc", CharsetUtil.UTF_8);
                  ByteBuf data = allocator.buffer();
                  data.writeCharSequence("def", CharsetUtil.UTF_8);
                  as.request(1);
                  int streamId = rule.getStreamIdForRequestType(REQUEST_STREAM);
                  ByteBuf frame =
                      PayloadFrameCodec.encode(
                          allocator, streamId, false, false, true, metadata, data);

                  RaceTestUtils.race(as::cancel, () -> rule.connection.addToReceivedBuffer(frame));
                }),
        Arguments.of(
            (Function<ClientSocketRule, Publisher<Payload>>)
                (rule) -> rule.socket.requestChannel(Flux.just(EmptyPayload.INSTANCE)),
            (BiConsumer<AssertSubscriber<Payload>, ClientSocketRule>)
                (as, rule) -> {
                  ByteBufAllocator allocator = rule.alloc();
                  ByteBuf metadata = allocator.buffer();
                  metadata.writeCharSequence("abc", CharsetUtil.UTF_8);
                  ByteBuf data = allocator.buffer();
                  data.writeCharSequence("def", CharsetUtil.UTF_8);
                  as.request(1);
                  int streamId = rule.getStreamIdForRequestType(REQUEST_CHANNEL);
                  ByteBuf frame =
                      PayloadFrameCodec.encode(
                          allocator, streamId, false, false, true, metadata, data);

                  RaceTestUtils.race(as::cancel, () -> rule.connection.addToReceivedBuffer(frame));
                }),
        Arguments.of(
            (Function<ClientSocketRule, Publisher<Payload>>)
                (rule) -> {
                  ByteBufAllocator allocator = rule.alloc();
                  ByteBuf metadata = allocator.buffer();
                  metadata.writeCharSequence("metadata", CharsetUtil.UTF_8);
                  ByteBuf data = allocator.buffer();
                  data.writeCharSequence("data", CharsetUtil.UTF_8);
                  final Payload payload = ByteBufPayload.create(data, metadata);

                  return rule.socket.requestStream(payload);
                },
            (BiConsumer<AssertSubscriber<Payload>, ClientSocketRule>)
                (as, rule) -> {
                  RaceTestUtils.race(() -> as.request(1), as::cancel);
                  // ensures proper frames order
                  if (rule.connection.getSent().size() > 0) {
                    Assertions.assertThat(rule.connection.getSent()).hasSize(2);
                    Assertions.assertThat(rule.connection.getSent())
                        .element(0)
                        .matches(
                            bb -> frameType(bb) == REQUEST_STREAM,
                            "Expected first frame matches {"
                                + REQUEST_STREAM
                                + "} but was {"
                                + frameType(rule.connection.getSent().stream().findFirst().get())
                                + "}");
                    Assertions.assertThat(rule.connection.getSent())
                        .element(1)
                        .matches(
                            bb -> frameType(bb) == CANCEL,
                            "Expected first frame matches {"
                                + CANCEL
                                + "} but was {"
                                + frameType(
                                    rule.connection.getSent().stream().skip(1).findFirst().get())
                                + "}");
                  }
                }),
        Arguments.of(
            (Function<ClientSocketRule, Publisher<Payload>>)
                (rule) -> {
                  ByteBufAllocator allocator = rule.alloc();
                  return rule.socket.requestChannel(
                      Flux.generate(
                          () -> 1L,
                          (index, sink) -> {
                            ByteBuf metadata = allocator.buffer();
                            metadata.writeCharSequence("metadata", CharsetUtil.UTF_8);
                            ByteBuf data = allocator.buffer();
                            data.writeCharSequence("data", CharsetUtil.UTF_8);
                            final Payload payload = ByteBufPayload.create(data, metadata);
                            sink.next(payload);
                            sink.complete();
                            return ++index;
                          }));
                },
            (BiConsumer<AssertSubscriber<Payload>, ClientSocketRule>)
                (as, rule) -> {
                  RaceTestUtils.race(() -> as.request(1), as::cancel);
                  // ensures proper frames order
                  int size = rule.connection.getSent().size();
                  if (size > 0) {

                    Assertions.assertThat(size).isLessThanOrEqualTo(3).isGreaterThanOrEqualTo(2);
                    Assertions.assertThat(rule.connection.getSent())
                        .element(0)
                        .matches(
                            bb -> frameType(bb) == REQUEST_CHANNEL,
                            "Expected first frame matches {"
                                + REQUEST_CHANNEL
                                + "} but was {"
                                + frameType(rule.connection.getSent().stream().findFirst().get())
                                + "}");
                    if (size == 2) {
                      Assertions.assertThat(rule.connection.getSent())
                          .element(1)
                          .matches(
                              bb -> frameType(bb) == CANCEL,
                              "Expected second frame matches {"
                                  + CANCEL
                                  + "} but was {"
                                  + frameType(
                                      rule.connection.getSent().stream().skip(1).findFirst().get())
                                  + "}");
                    } else {
                      Assertions.assertThat(rule.connection.getSent())
                          .element(1)
                          .matches(
                              bb -> frameType(bb) == COMPLETE || frameType(bb) == CANCEL,
                              "Expected second frame matches {"
                                  + COMPLETE
                                  + " or "
                                  + CANCEL
                                  + "} but was {"
                                  + frameType(
                                      rule.connection.getSent().stream().skip(1).findFirst().get())
                                  + "}");
                      Assertions.assertThat(rule.connection.getSent())
                          .element(2)
                          .matches(
                              bb -> frameType(bb) == CANCEL || frameType(bb) == COMPLETE,
                              "Expected third frame matches {"
                                  + COMPLETE
                                  + " or "
                                  + CANCEL
                                  + "} but was {"
                                  + frameType(
                                      rule.connection.getSent().stream().skip(2).findFirst().get())
                                  + "}");
                    }
                  }
                }),
        Arguments.of(
            (Function<ClientSocketRule, Publisher<Payload>>)
                (rule) ->
                    rule.socket.requestChannel(
                        Flux.generate(
                            () -> 1L,
                            (index, sink) -> {
                              ByteBuf data = rule.alloc().buffer();
                              data.writeCharSequence("d" + index, CharsetUtil.UTF_8);
                              ByteBuf metadata = rule.alloc().buffer();
                              metadata.writeCharSequence("m" + index, CharsetUtil.UTF_8);
                              final Payload payload = ByteBufPayload.create(data, metadata);
                              sink.next(payload);
                              return ++index;
                            })),
            (BiConsumer<AssertSubscriber<Payload>, ClientSocketRule>)
                (as, rule) -> {
                  ByteBufAllocator allocator = rule.alloc();
                  as.request(1);
                  int streamId = rule.getStreamIdForRequestType(REQUEST_CHANNEL);
                  ByteBuf frame = CancelFrameCodec.encode(allocator, streamId);

                  RaceTestUtils.race(
                      () -> as.request(Long.MAX_VALUE),
                      () -> rule.connection.addToReceivedBuffer(frame));
                }),
        Arguments.of(
            (Function<ClientSocketRule, Publisher<Payload>>)
                (rule) ->
                    rule.socket.requestChannel(
                        Flux.generate(
                            () -> 1L,
                            (index, sink) -> {
                              ByteBuf data = rule.alloc().buffer();
                              data.writeCharSequence("d" + index, CharsetUtil.UTF_8);
                              ByteBuf metadata = rule.alloc().buffer();
                              metadata.writeCharSequence("m" + index, CharsetUtil.UTF_8);
                              final Payload payload = ByteBufPayload.create(data, metadata);
                              sink.next(payload);
                              return ++index;
                            })),
            (BiConsumer<AssertSubscriber<Payload>, ClientSocketRule>)
                (as, rule) -> {
                  ByteBufAllocator allocator = rule.alloc();
                  as.request(1);
                  int streamId = rule.getStreamIdForRequestType(REQUEST_CHANNEL);
                  ByteBuf frame =
                      ErrorFrameCodec.encode(allocator, streamId, new RuntimeException("test"));

                  RaceTestUtils.race(
                      () -> as.request(Long.MAX_VALUE),
                      () -> rule.connection.addToReceivedBuffer(frame));
                }),
        Arguments.of(
            (Function<ClientSocketRule, Publisher<Payload>>)
                (rule) -> {
                  ByteBuf data = rule.allocator.buffer();
                  data.writeCharSequence("testData", CharsetUtil.UTF_8);

                  ByteBuf metadata = rule.allocator.buffer();
                  metadata.writeCharSequence("testMetadata", CharsetUtil.UTF_8);
                  Payload requestPayload = ByteBufPayload.create(data, metadata);
                  return rule.socket.requestResponse(requestPayload);
                },
            (BiConsumer<AssertSubscriber<Payload>, ClientSocketRule>)
                (as, rule) -> {
                  ByteBufAllocator allocator = rule.alloc();
                  ByteBuf metadata = allocator.buffer();
                  metadata.writeCharSequence("abc", CharsetUtil.UTF_8);
                  ByteBuf data = allocator.buffer();
                  data.writeCharSequence("def", CharsetUtil.UTF_8);
                  as.request(Long.MAX_VALUE);
                  int streamId = rule.getStreamIdForRequestType(REQUEST_RESPONSE);
                  ByteBuf frame =
                      PayloadFrameCodec.encode(
                          allocator, streamId, false, false, true, metadata, data);

                  RaceTestUtils.race(as::cancel, () -> rule.connection.addToReceivedBuffer(frame));
                }),
        Arguments.of(
            (Function<ClientSocketRule, Publisher<Payload>>)
                (rule) -> {
                  ByteBuf data = rule.allocator.buffer();
                  data.writeCharSequence("testData", CharsetUtil.UTF_8);

                  ByteBuf metadata = rule.allocator.buffer();
                  metadata.writeCharSequence("testMetadata", CharsetUtil.UTF_8);
                  Payload requestPayload = ByteBufPayload.create(data, metadata);
                  return rule.socket.requestStream(requestPayload);
                },
            (BiConsumer<AssertSubscriber<Payload>, ClientSocketRule>)
                (as, rule) -> {
                  ByteBufAllocator allocator = rule.alloc();
                  ByteBuf metadata = allocator.buffer();
                  metadata.writeCharSequence("abc", CharsetUtil.UTF_8);
                  ByteBuf data = allocator.buffer();
                  data.writeCharSequence("def", CharsetUtil.UTF_8);
                  as.request(Long.MAX_VALUE);
                  int streamId = rule.getStreamIdForRequestType(REQUEST_STREAM);
                  ByteBuf frame =
                      PayloadFrameCodec.encode(
                          allocator, streamId, false, true, true, metadata, data);

                  RaceTestUtils.race(as::cancel, () -> rule.connection.addToReceivedBuffer(frame));
                }));
  }

  @Test
  public void simpleOnDiscardRequestChannelTest() {
    AssertSubscriber<Payload> assertSubscriber = AssertSubscriber.create(1);
    TestPublisher<Payload> testPublisher = TestPublisher.create();

    Flux<Payload> payloadFlux = rule.socket.requestChannel(testPublisher);

    payloadFlux.subscribe(assertSubscriber);

    testPublisher.next(
        ByteBufPayload.create("d", "m"),
        ByteBufPayload.create("d1", "m1"),
        ByteBufPayload.create("d2", "m2"));

    assertSubscriber.cancel();

    Assertions.assertThat(rule.connection.getSent()).allMatch(ByteBuf::release);

    rule.assertHasNoLeaks();
  }

  @Test
  public void simpleOnDiscardRequestChannelTest2() {
    ByteBufAllocator allocator = rule.alloc();
    AssertSubscriber<Payload> assertSubscriber = AssertSubscriber.create(1);
    TestPublisher<Payload> testPublisher = TestPublisher.create();

    Flux<Payload> payloadFlux = rule.socket.requestChannel(testPublisher);

    payloadFlux.subscribe(assertSubscriber);

    testPublisher.next(ByteBufPayload.create("d", "m"));

    int streamId = rule.getStreamIdForRequestType(REQUEST_CHANNEL);
    testPublisher.next(ByteBufPayload.create("d1", "m1"), ByteBufPayload.create("d2", "m2"));

    rule.connection.addToReceivedBuffer(
        ErrorFrameCodec.encode(
            allocator, streamId, new CustomRSocketException(0x00000404, "test")));

    Assertions.assertThat(rule.connection.getSent()).allMatch(ByteBuf::release);

    rule.assertHasNoLeaks();
  }

  @ParameterizedTest
  @MethodSource("encodeDecodePayloadCases")
  public void verifiesThatFrameWithNoMetadataHasDecodedCorrectlyIntoPayload(
      FrameType frameType, int framesCnt, int responsesCnt) {
    ByteBufAllocator allocator = rule.alloc();
    AssertSubscriber<Payload> assertSubscriber = AssertSubscriber.create(responsesCnt);
    TestPublisher<Payload> testPublisher = TestPublisher.create();

    Publisher<Payload> response;

    switch (frameType) {
      case REQUEST_FNF:
        response =
            testPublisher.mono().flatMap(p -> rule.socket.fireAndForget(p).then(Mono.empty()));
        break;
      case REQUEST_RESPONSE:
        response = testPublisher.mono().flatMap(p -> rule.socket.requestResponse(p));
        break;
      case REQUEST_STREAM:
        response = testPublisher.mono().flatMapMany(p -> rule.socket.requestStream(p));
        break;
      case REQUEST_CHANNEL:
        response = rule.socket.requestChannel(testPublisher.flux());
        break;
      default:
        throw new UnsupportedOperationException("illegal case");
    }

    response.subscribe(assertSubscriber);
    testPublisher.next(ByteBufPayload.create("d"));

    int streamId = rule.getStreamIdForRequestType(frameType);

    if (responsesCnt > 0) {
      for (int i = 0; i < responsesCnt - 1; i++) {
        rule.connection.addToReceivedBuffer(
            PayloadFrameCodec.encode(
                allocator,
                streamId,
                false,
                false,
                true,
                null,
                Unpooled.wrappedBuffer(("rd" + (i + 1)).getBytes())));
      }

      rule.connection.addToReceivedBuffer(
          PayloadFrameCodec.encode(
              allocator,
              streamId,
              false,
              true,
              true,
              null,
              Unpooled.wrappedBuffer(("rd" + responsesCnt).getBytes())));
    }

    if (framesCnt > 1) {
      rule.connection.addToReceivedBuffer(
          RequestNFrameCodec.encode(allocator, streamId, framesCnt));
    }

    for (int i = 1; i < framesCnt; i++) {
      testPublisher.next(ByteBufPayload.create("d" + i));
    }

    Assertions.assertThat(rule.connection.getSent())
        .describedAs(
            "Interaction Type :[%s]. Expected to observe %s frames sent", frameType, framesCnt)
        .hasSize(framesCnt)
        .allMatch(bb -> !FrameHeaderCodec.hasMetadata(bb))
        .allMatch(ByteBuf::release);

    Assertions.assertThat(assertSubscriber.isTerminated())
        .describedAs("Interaction Type :[%s]. Expected to be terminated", frameType)
        .isTrue();

    Assertions.assertThat(assertSubscriber.values())
        .describedAs(
            "Interaction Type :[%s]. Expected to observe %s frames received",
            frameType, responsesCnt)
        .hasSize(responsesCnt)
        .allMatch(p -> !p.hasMetadata())
        .allMatch(p -> p.release());

    rule.assertHasNoLeaks();
    rule.connection.clearSendReceiveBuffers();
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
  public void ensureSendsErrorOnIllegalRefCntPayload(
      BiFunction<Payload, ClientSocketRule, Publisher<?>> sourceProducer) {
    Payload invalidPayload = ByteBufPayload.create("test", "test");
    invalidPayload.release();

    Publisher<?> source = sourceProducer.apply(invalidPayload, rule);

    StepVerifier.create(source, 1)
        .expectError(IllegalReferenceCountException.class)
        .verify(Duration.ofMillis(1000));
  }

  private static Stream<BiFunction<Payload, ClientSocketRule, Publisher<?>>> refCntCases() {
    return Stream.of(
        (p, clientSocketRule) -> clientSocketRule.socket.fireAndForget(p),
        (p, clientSocketRule) -> clientSocketRule.socket.requestResponse(p),
        (p, clientSocketRule) -> clientSocketRule.socket.requestStream(p),
        (p, clientSocketRule) -> clientSocketRule.socket.requestChannel(Mono.just(p)),
        (p, clientSocketRule) -> {
          Flux.from(clientSocketRule.connection.getSentAsPublisher())
              .filter(bb -> FrameHeaderCodec.frameType(bb) == REQUEST_CHANNEL)
              .subscribe(
                  bb -> {
                    clientSocketRule.connection.addToReceivedBuffer(
                        RequestNFrameCodec.encode(
                            clientSocketRule.allocator, FrameHeaderCodec.streamId(bb), 1));
                    bb.release();
                  });

          return clientSocketRule.socket.requestChannel(Flux.just(EmptyPayload.INSTANCE, p));
        });
  }

  @Test
  public void ensuresThatNoOpsMustHappenUntilSubscriptionInCaseOfFnfCall() {
    Payload payload1 = ByteBufPayload.create("abc1");
    Mono<Void> fnf1 = rule.socket.fireAndForget(payload1);

    Payload payload2 = ByteBufPayload.create("abc2");
    Mono<Void> fnf2 = rule.socket.fireAndForget(payload2);

    Assertions.assertThat(rule.connection.getSent()).isEmpty();

    // checks that fnf2 should have id 1 even though it was generated later than fnf1
    AssertSubscriber<Void> voidAssertSubscriber2 = fnf2.subscribeWith(AssertSubscriber.create(0));
    voidAssertSubscriber2.assertTerminated().assertNoError();
    Assertions.assertThat(rule.connection.getSent())
        .hasSize(1)
        .first()
        .matches(bb -> frameType(bb) == REQUEST_FNF)
        .matches(bb -> FrameHeaderCodec.streamId(bb) == 1)
        // ensures that this is fnf1 with abc2 data
        .matches(
            bb ->
                ByteBufUtil.equals(
                    RequestFireAndForgetFrameCodec.data(bb),
                    Unpooled.wrappedBuffer("abc2".getBytes())))
        .matches(ReferenceCounted::release);

    rule.connection.clearSendReceiveBuffers();

    // checks that fnf1 should have id 3 even though it was generated earlier
    AssertSubscriber<Void> voidAssertSubscriber1 = fnf1.subscribeWith(AssertSubscriber.create(0));
    voidAssertSubscriber1.assertTerminated().assertNoError();
    Assertions.assertThat(rule.connection.getSent())
        .hasSize(1)
        .first()
        .matches(bb -> frameType(bb) == REQUEST_FNF)
        .matches(bb -> FrameHeaderCodec.streamId(bb) == 3)
        // ensures that this is fnf1 with abc1 data
        .matches(
            bb ->
                ByteBufUtil.equals(
                    RequestFireAndForgetFrameCodec.data(bb),
                    Unpooled.wrappedBuffer("abc1".getBytes())))
        .matches(ReferenceCounted::release);
  }

  @ParameterizedTest
  @MethodSource("requestNInteractions")
  public void ensuresThatNoOpsMustHappenUntilFirstRequestN(
      FrameType frameType, BiFunction<ClientSocketRule, Payload, Publisher<Payload>> interaction) {
    Payload payload1 = ByteBufPayload.create("abc1");
    Publisher<Payload> interaction1 = interaction.apply(rule, payload1);

    Payload payload2 = ByteBufPayload.create("abc2");
    Publisher<Payload> interaction2 = interaction.apply(rule, payload2);

    Assertions.assertThat(rule.connection.getSent()).isEmpty();

    AssertSubscriber<Payload> assertSubscriber1 = AssertSubscriber.create(0);
    interaction1.subscribe(assertSubscriber1);
    AssertSubscriber<Payload> assertSubscriber2 = AssertSubscriber.create(0);
    interaction2.subscribe(assertSubscriber2);
    assertSubscriber1.assertNotTerminated().assertNoError();
    assertSubscriber2.assertNotTerminated().assertNoError();
    // even though we subscribed, nothing should happen until the first requestN
    Assertions.assertThat(rule.connection.getSent()).isEmpty();

    // first request on the second interaction to ensure that stream id issuing on the first request
    assertSubscriber2.request(1);

    Assertions.assertThat(rule.connection.getSent())
        .hasSize(frameType == REQUEST_CHANNEL ? 2 : 1)
        .first()
        .matches(bb -> frameType(bb) == frameType)
        .matches(
            bb -> FrameHeaderCodec.streamId(bb) == 1,
            "Expected to have stream ID {1} but got {"
                + FrameHeaderCodec.streamId(rule.connection.getSent().iterator().next())
                + "}")
        .matches(
            bb -> {
              switch (frameType) {
                case REQUEST_RESPONSE:
                  return ByteBufUtil.equals(
                      RequestResponseFrameCodec.data(bb),
                      Unpooled.wrappedBuffer("abc2".getBytes()));
                case REQUEST_STREAM:
                  return ByteBufUtil.equals(
                      RequestStreamFrameCodec.data(bb), Unpooled.wrappedBuffer("abc2".getBytes()));
                case REQUEST_CHANNEL:
                  return ByteBufUtil.equals(
                      RequestChannelFrameCodec.data(bb), Unpooled.wrappedBuffer("abc2".getBytes()));
              }

              return false;
            })
        .matches(ReferenceCounted::release);

    if (frameType == REQUEST_CHANNEL) {
      Assertions.assertThat(rule.connection.getSent())
          .element(1)
          .matches(bb -> frameType(bb) == COMPLETE)
          .matches(
              bb -> FrameHeaderCodec.streamId(bb) == 1,
              "Expected to have stream ID {1} but got {"
                  + FrameHeaderCodec.streamId(new ArrayList<>(rule.connection.getSent()).get(1))
                  + "}")
          .matches(ReferenceCounted::release);
    }

    rule.connection.clearSendReceiveBuffers();

    assertSubscriber1.request(1);
    Assertions.assertThat(rule.connection.getSent())
        .hasSize(frameType == REQUEST_CHANNEL ? 2 : 1)
        .first()
        .matches(bb -> frameType(bb) == frameType)
        .matches(
            bb -> FrameHeaderCodec.streamId(bb) == 3,
            "Expected to have stream ID {1} but got {"
                + FrameHeaderCodec.streamId(rule.connection.getSent().iterator().next())
                + "}")
        .matches(
            bb -> {
              switch (frameType) {
                case REQUEST_RESPONSE:
                  return ByteBufUtil.equals(
                      RequestResponseFrameCodec.data(bb),
                      Unpooled.wrappedBuffer("abc1".getBytes()));
                case REQUEST_STREAM:
                  return ByteBufUtil.equals(
                      RequestStreamFrameCodec.data(bb), Unpooled.wrappedBuffer("abc1".getBytes()));
                case REQUEST_CHANNEL:
                  return ByteBufUtil.equals(
                      RequestChannelFrameCodec.data(bb), Unpooled.wrappedBuffer("abc1".getBytes()));
              }

              return false;
            })
        .matches(ReferenceCounted::release);

    if (frameType == REQUEST_CHANNEL) {
      Assertions.assertThat(rule.connection.getSent())
          .element(1)
          .matches(bb -> frameType(bb) == COMPLETE)
          .matches(
              bb -> FrameHeaderCodec.streamId(bb) == 3,
              "Expected to have stream ID {1} but got {"
                  + FrameHeaderCodec.streamId(new ArrayList<>(rule.connection.getSent()).get(1))
                  + "}")
          .matches(ReferenceCounted::release);
    }
  }

  private static Stream<Arguments> requestNInteractions() {
    return Stream.of(
        Arguments.of(
            REQUEST_RESPONSE,
            (BiFunction<ClientSocketRule, Payload, Publisher<Payload>>)
                (rule, payload) -> rule.socket.requestResponse(payload)),
        Arguments.of(
            REQUEST_STREAM,
            (BiFunction<ClientSocketRule, Payload, Publisher<Payload>>)
                (rule, payload) -> rule.socket.requestStream(payload)),
        Arguments.of(
            REQUEST_CHANNEL,
            (BiFunction<ClientSocketRule, Payload, Publisher<Payload>>)
                (rule, payload) -> rule.socket.requestChannel(Flux.just(payload))));
  }

  @ParameterizedTest
  @MethodSource("streamRacingCases")
  @Disabled("Connection should take care of ordering if such is necessary")
  public void ensuresCorrectOrderOfStreamIdIssuingInCaseOfRacing(
      BiFunction<ClientSocketRule, Payload, Publisher<?>> interaction1,
      BiFunction<ClientSocketRule, Payload, Publisher<?>> interaction2,
      FrameType interactionType1,
      FrameType interactionType2) {
    Assumptions.assumeThat(interactionType1).isNotEqualTo(METADATA_PUSH);
    Assumptions.assumeThat(interactionType2).isNotEqualTo(METADATA_PUSH);
    for (int i = 1; i < 10000; i += 4) {
      Payload payload = DefaultPayload.create("test", "test");
      Publisher<?> publisher1 = interaction1.apply(rule, payload);
      Publisher<?> publisher2 = interaction2.apply(rule, payload);
      RaceTestUtils.race(
          () -> publisher1.subscribe(AssertSubscriber.create()),
          () -> publisher2.subscribe(AssertSubscriber.create()));

      Assertions.assertThat(rule.connection.getSent())
          .extracting(FrameHeaderCodec::streamId)
          .containsExactly(i, i + 2);
      rule.connection.getSent().forEach(bb -> bb.release());
      rule.connection.getSent().clear();
    }
  }

  public static Stream<Arguments> streamRacingCases() {
    return Stream.of(
        Arguments.of(
            (BiFunction<ClientSocketRule, Payload, Publisher<?>>)
                (r, p) -> r.socket.fireAndForget(p),
            (BiFunction<ClientSocketRule, Payload, Publisher<?>>)
                (r, p) -> r.socket.requestResponse(p),
            REQUEST_FNF,
            REQUEST_RESPONSE),
        Arguments.of(
            (BiFunction<ClientSocketRule, Payload, Publisher<?>>)
                (r, p) -> r.socket.requestResponse(p),
            (BiFunction<ClientSocketRule, Payload, Publisher<?>>)
                (r, p) -> r.socket.requestStream(p),
            REQUEST_RESPONSE,
            REQUEST_STREAM),
        Arguments.of(
            (BiFunction<ClientSocketRule, Payload, Publisher<?>>)
                (r, p) -> r.socket.requestStream(p),
            (BiFunction<ClientSocketRule, Payload, Publisher<?>>)
                (r, p) -> {
                  AtomicBoolean subscribed = new AtomicBoolean();
                  Flux<Payload> just = Flux.just(p).doOnSubscribe((__) -> subscribed.set(true));
                  return r.socket
                      .requestChannel(just)
                      .doFinally(
                          __ -> {
                            if (!subscribed.get()) {
                              p.release();
                            }
                          });
                },
            REQUEST_STREAM,
            REQUEST_CHANNEL),
        Arguments.of(
            (BiFunction<ClientSocketRule, Payload, Publisher<?>>)
                (r, p) -> {
                  AtomicBoolean subscribed = new AtomicBoolean();
                  Flux<Payload> just = Flux.just(p).doOnSubscribe((__) -> subscribed.set(true));
                  return r.socket
                      .requestChannel(just)
                      .doFinally(
                          __ -> {
                            if (!subscribed.get()) {
                              p.release();
                            }
                          });
                },
            (BiFunction<ClientSocketRule, Payload, Publisher<?>>)
                (r, p) -> r.socket.fireAndForget(p),
            REQUEST_CHANNEL,
            REQUEST_FNF),
        Arguments.of(
            (BiFunction<ClientSocketRule, Payload, Publisher<?>>)
                (r, p) -> r.socket.metadataPush(p),
            (BiFunction<ClientSocketRule, Payload, Publisher<?>>)
                (r, p) -> r.socket.fireAndForget(p),
            METADATA_PUSH,
            REQUEST_FNF));
  }

  @ParameterizedTest
  @MethodSource("streamRacingCases")
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void shouldTerminateAllStreamsIfThereRacingBetweenDisposeAndRequests(
      BiFunction<ClientSocketRule, Payload, Publisher<?>> interaction1,
      BiFunction<ClientSocketRule, Payload, Publisher<?>> interaction2,
      FrameType interactionType1,
      FrameType interactionType2) {
    for (int i = 1; i < 10000; i++) {
      Payload payload1 = ByteBufPayload.create("test", "test");
      Payload payload2 = ByteBufPayload.create("test", "test");
      AssertSubscriber assertSubscriber1 = AssertSubscriber.create();
      AssertSubscriber assertSubscriber2 = AssertSubscriber.create();
      Publisher<?> publisher1 = interaction1.apply(rule, payload1);
      Publisher<?> publisher2 = interaction2.apply(rule, payload2);
      RaceTestUtils.race(
          () -> rule.socket.dispose(),
          () ->
              RaceTestUtils.race(
                  () -> publisher1.subscribe(assertSubscriber1),
                  () -> publisher2.subscribe(assertSubscriber2),
                  Schedulers.parallel()),
          Schedulers.parallel());

      assertSubscriber1.await().assertTerminated();
      if (interactionType1 != REQUEST_FNF && interactionType1 != METADATA_PUSH) {
        assertSubscriber1.assertError(ClosedChannelException.class);
      } else {
        try {
          assertSubscriber1.assertError(ClosedChannelException.class);
        } catch (Throwable t) {
          // fnf call may be completed
          assertSubscriber1.assertComplete();
        }
      }
      assertSubscriber2.await().assertTerminated();
      if (interactionType2 != REQUEST_FNF && interactionType2 != METADATA_PUSH) {
        assertSubscriber2.assertError(ClosedChannelException.class);
      } else {
        try {
          assertSubscriber2.assertError(ClosedChannelException.class);
        } catch (Throwable t) {
          // fnf call may be completed
          assertSubscriber2.assertComplete();
        }
      }

      Assertions.assertThat(rule.connection.getSent()).allMatch(ReferenceCounted::release);
      rule.connection.getSent().clear();

      Assertions.assertThat(payload1.refCnt()).isZero();
      Assertions.assertThat(payload2.refCnt()).isZero();
    }
  }

  @Test
  @Disabled("Reactor 3.4.0 should fix that. No need to do anything on our side")
  // see https://github.com/rsocket/rsocket-java/issues/858
  public void testWorkaround858() {
    ByteBuf buffer = rule.alloc().buffer();
    buffer.writeCharSequence("test", CharsetUtil.UTF_8);

    rule.socket.requestResponse(ByteBufPayload.create(buffer)).subscribe();

    rule.connection.addToReceivedBuffer(
        ErrorFrameCodec.encode(rule.alloc(), 1, new RuntimeException("test")));

    Assertions.assertThat(rule.connection.getSent())
        .hasSize(1)
        .first()
        .matches(bb -> FrameHeaderCodec.frameType(bb) == REQUEST_RESPONSE)
        .matches(ByteBuf::release);

    Assertions.assertThat(rule.socket.isDisposed()).isFalse();

    rule.assertHasNoLeaks();
  }

  @DisplayName("reassembles data")
  @ParameterizedTest
  @MethodSource("requestNInteractions")
  void reassembleData(
      FrameType frameType,
      BiFunction<ClientSocketRule, Payload, Publisher<Payload>> requestFunction) {
    final int mtu = ThreadLocalRandom.current().nextInt(64, 256);
    final LeaksTrackingByteBufAllocator leaksTrackingByteBufAllocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    final Payload requestPayload = genericPayload(leaksTrackingByteBufAllocator);
    final Payload randomPayload = randomPayload(leaksTrackingByteBufAllocator);
    List<ByteBuf> fragments = prepareFragments(leaksTrackingByteBufAllocator, mtu, randomPayload);

    final Publisher<Payload> responsePublisher = requestFunction.apply(rule, requestPayload);
    StepVerifier.create(responsePublisher)
        .then(() -> rule.connection.addToReceivedBuffer(fragments.toArray(new ByteBuf[0])))
        .assertNext(
            p -> {
              PayloadAssert.assertThat(p).isEqualTo(randomPayload).hasNoLeaks();
              randomPayload.release();
            })
        .thenCancel()
        .verify();

    FrameAssert.assertThat(rule.connection.getSent().poll()).typeOf(frameType).hasNoLeaks();

    if (frameType == REQUEST_CHANNEL) {
      FrameAssert.assertThat(rule.connection.getSent().poll()).typeOf(COMPLETE).hasNoLeaks();
    }

    if (!rule.connection.getSent().isEmpty()) {
      FrameAssert.assertThat(rule.connection.getSent().poll()).typeOf(CANCEL).hasNoLeaks();
    }

    leaksTrackingByteBufAllocator.assertHasNoLeaks();
  }

  @DisplayName("reassembles metadata")
  @ParameterizedTest
  @MethodSource("requestNInteractions")
  void reassembleMetadata(
      FrameType frameType,
      BiFunction<ClientSocketRule, Payload, Publisher<Payload>> requestFunction) {
    final int mtu = ThreadLocalRandom.current().nextInt(64, 256);
    final LeaksTrackingByteBufAllocator leaksTrackingByteBufAllocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);

    final Payload requestPayload = genericPayload(leaksTrackingByteBufAllocator);
    final Payload metadataOnlyPayload = randomMetadataOnlyPayload(leaksTrackingByteBufAllocator);
    List<ByteBuf> fragments =
        prepareFragments(leaksTrackingByteBufAllocator, mtu, metadataOnlyPayload);

    StepVerifier.create(requestFunction.apply(rule, requestPayload))
        .then(() -> rule.connection.addToReceivedBuffer(fragments.toArray(new ByteBuf[0])))
        .assertNext(
            responsePayload -> {
              PayloadAssert.assertThat(requestPayload).isEqualTo(metadataOnlyPayload).hasNoLeaks();
              metadataOnlyPayload.release();
            })
        .thenCancel()
        .verify();

    FrameAssert.assertThat(rule.connection.getSent().poll()).typeOf(frameType).hasNoLeaks();

    if (frameType == REQUEST_CHANNEL) {
      FrameAssert.assertThat(rule.connection.getSent().poll()).typeOf(COMPLETE).hasNoLeaks();
    }

    if (!rule.connection.getSent().isEmpty()) {
      FrameAssert.assertThat(rule.connection.getSent().poll()).typeOf(CANCEL).hasNoLeaks();
    }

    leaksTrackingByteBufAllocator.assertHasNoLeaks();
  }

  @ParameterizedTest(name = "throws error if reassembling payload size exceeds {0}")
  @MethodSource("requestNInteractions")
  public void errorTooBigPayload(
      FrameType frameType,
      BiFunction<ClientSocketRule, Payload, Publisher<Payload>> requestFunction) {
    final int maxInboundPayloadSize = ThreadLocalRandom.current().nextInt(64, 4096);
    final int mtu = ThreadLocalRandom.current().nextInt(64, 256);
    final LeaksTrackingByteBufAllocator leaksTrackingByteBufAllocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);

    final Payload requestPayload = genericPayload(leaksTrackingByteBufAllocator);
    final Payload responsePayload =
        fixedSizePayload(leaksTrackingByteBufAllocator, maxInboundPayloadSize + 1);
    List<ByteBuf> fragments = prepareFragments(leaksTrackingByteBufAllocator, mtu, responsePayload);
    responsePayload.release();

    rule.setMaxInboundPayloadSize(maxInboundPayloadSize);

    StepVerifier.create(requestFunction.apply(rule, requestPayload))
        .then(() -> rule.connection.addToReceivedBuffer(fragments.toArray(new ByteBuf[0])))
        .expectErrorMessage(String.format(ILLEGAL_REASSEMBLED_PAYLOAD_SIZE, maxInboundPayloadSize))
        .verify();

    FrameAssert.assertThat(rule.connection.getSent().poll()).typeOf(frameType).hasNoLeaks();

    if (frameType == REQUEST_CHANNEL) {
      FrameAssert.assertThat(rule.connection.getSent().poll()).typeOf(COMPLETE).hasNoLeaks();
    }

    FrameAssert.assertThat(rule.connection.getSent().poll()).typeOf(CANCEL).hasNoLeaks();

    leaksTrackingByteBufAllocator.assertHasNoLeaks();
  }

  public static class ClientSocketRule extends AbstractSocketRule<RSocketRequester> {
    @Override
    protected RSocketRequester newRSocket() {
      return new RSocketRequester(
          connection,
          PayloadDecoder.ZERO_COPY,
          StreamIdSupplier.clientSupplier(),
          0,
          maxFrameLength,
          maxInboundPayloadSize,
          Integer.MAX_VALUE,
          Integer.MAX_VALUE,
          null,
          RequesterLeaseHandler.None);
    }

    public int getStreamIdForRequestType(FrameType expectedFrameType) {
      assertThat("Unexpected frames sent.", connection.getSent(), hasSize(greaterThanOrEqualTo(1)));
      List<FrameType> framesFound = new ArrayList<>();
      for (ByteBuf frame : connection.getSent()) {
        FrameType frameType = frameType(frame);
        if (frameType == expectedFrameType) {
          return FrameHeaderCodec.streamId(frame);
        }
        framesFound.add(frameType);
      }
      throw new AssertionError(
          "No frames sent with frame type: "
              + expectedFrameType
              + ", frames found: "
              + framesFound);
    }
  }
}
